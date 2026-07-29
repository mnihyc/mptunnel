//! Generation-scoped split DNS runtime.
//!
//! Explicit upstreams are always dialed through their literal bootstrap IP.
//! This module never loads system resolver configuration or the hosts file.

use crate::product::{
    CompiledDnsPlan, CompiledDnsPolicy, CompiledDnsUpstream, DnsEgressSpec, DnsIpStrategy,
    DnsPlanId, DnsPlanLimits, DnsRuleId, DnsRuleMatchKind, DnsTransport, DnsUpstreamEndpoint,
    DnsUpstreamId, DnsUpstreamStrategy, DomainName, FakeDnsSpec, OutboundId, ProductAdmission,
    ProductAdmissionRejection, ProductDnsWork,
};
use crate::transport::{
    NativeEgressPurpose, NativeSocketConfigurator, NativeSocketRequest,
    SystemNativeSocketConfigurator,
};
use bytes::Bytes;
use futures::StreamExt;
use futures::stream::FuturesUnordered;
use hickory_proto::op::{DnsResponse, Edns, Message, MessageType, OpCode, Query, ResponseCode};
use hickory_proto::rr::rdata::{A, AAAA};
use hickory_proto::rr::{DNSClass, Name, RData, Record, RecordType};
use hickory_resolver::Resolver;
use hickory_resolver::config::{
    ConnectionConfig, LookupIpStrategy, NameServerConfig, ProtocolConfig, ResolveHosts,
    ResolverConfig, ResolverOpts, ServerOrderingStrategy,
};
use hickory_resolver::net::runtime::{
    QuicSocketBinder, RuntimeProvider, TokioHandle, TokioTime, iocompat::AsyncIoTokioAsStd,
};
use hickory_resolver::net::{DnsError as HickoryDnsError, NetError};
use http::{Method, Request, Version, header};
use ipnet::IpNet;
use rustls::pki_types::ServerName;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::future::Future;
use std::io;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr, UdpSocket as StdUdpSocket};
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::{TcpSocket, TcpStream, UdpSocket};
use tokio::sync::{Notify, OwnedSemaphorePermit, Semaphore};
use tokio_rustls::TlsConnector;

const SYSTEM_POSITIVE_FALLBACK_TTL: Duration = Duration::from_secs(60);
const NEGATIVE_FALLBACK_TTL: Duration = Duration::from_secs(5);
const MAX_BACKEND_ERROR_BYTES: usize = 512;
const MAX_DOH_RESPONSE_BYTES: usize = 65_535;
const MAX_DNS_WIRE_MESSAGE_BYTES: usize = 65_535;
const MAX_DNS_CACHED_RECORDS: usize = 256;
const DNS_MESSAGE_CONTENT_TYPE: &str = "application/dns-message";
const DEFAULT_DNS_STALE_ANSWER_TTL: Duration = Duration::from_secs(30);

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DnsBackendError {
    Timeout,
    NoRecords { ttl: Option<Duration> },
    Failed(String),
}

/// One normalized IN-class DNS question. Runtime caches and in-flight
/// coalescing use this complete key, so local wire queries and dial-time
/// address resolution cannot disagree about an upstream answer.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct DnsQuestion {
    domain: DomainName,
    record_type: RecordType,
}

impl DnsQuestion {
    pub const fn new(domain: DomainName, record_type: RecordType) -> Self {
        Self {
            domain,
            record_type,
        }
    }

    pub const fn domain(&self) -> &DomainName {
        &self.domain
    }

    pub const fn record_type(&self) -> RecordType {
        self.record_type
    }

    fn as_query(&self) -> Result<Query, DnsBackendError> {
        let name = Name::from_ascii(format!("{}.", self.domain))
            .map_err(|error| DnsBackendError::Failed(error.to_string()))?;
        Ok(Query::query(name, self.record_type))
    }
}

/// One upstream response candidate. The generation validates its question,
/// response code, section sizes, and wire size before caching it; request-local
/// transaction and EDNS metadata are supplied only when rendering.
#[derive(Debug, Clone)]
pub struct DnsBackendResponse {
    message: Message,
    ttl: Option<Duration>,
}

impl DnsBackendResponse {
    pub fn new(message: Message, ttl: Option<Duration>) -> Self {
        Self { message, ttl }
    }

    pub const fn message(&self) -> &Message {
        &self.message
    }

    pub const fn ttl(&self) -> Option<Duration> {
        self.ttl
    }
}

pub type DnsRecordBackendFuture =
    Pin<Box<dyn Future<Output = Result<DnsBackendResponse, DnsBackendError>> + Send + 'static>>;

/// One already-bound upstream query implementation.
///
/// Routed backends implement this trait using a pinned Product leaf. Returning
/// a backend from the factory is the explicit proof that the named egress was
/// honored; the direct factory never accepts one.
pub trait DnsQueryBackend: Send + Sync {
    fn query(&self, question: DnsQuestion) -> DnsRecordBackendFuture;
}

pub trait DnsBackendFactory: Send + Sync {
    fn build_backend(
        &self,
        plan: &CompiledDnsPlan,
        upstream: &CompiledDnsUpstream,
    ) -> Result<Arc<dyn DnsQueryBackend>, DnsRuntimeError>;
}

pub(crate) trait DnsTcpIo: AsyncRead + AsyncWrite + Unpin + Send {}

impl<T> DnsTcpIo for T where T: AsyncRead + AsyncWrite + Unpin + Send {}

pub(crate) type DnsTcpStream = Box<dyn DnsTcpIo>;
pub(crate) type DnsTcpConnectFuture =
    Pin<Box<dyn Future<Output = Result<DnsTcpStream, DnsBackendError>> + Send + 'static>>;

/// A DNS-independent TCP branch selected while an immutable resolver
/// generation is compiled.
///
/// Implementations receive only a literal upstream bootstrap address. They
/// must not consult system DNS or the Product resolver they are helping build.
pub(crate) trait DnsTcpConnector: Send + Sync {
    fn connect(&self, bootstrap: SocketAddr, timeout: Duration) -> DnsTcpConnectFuture;
}

#[derive(Clone)]
struct DirectDnsTcpConnector {
    policy: DnsNativeSocketPolicy,
}

impl DnsTcpConnector for DirectDnsTcpConnector {
    fn connect(&self, bootstrap: SocketAddr, timeout: Duration) -> DnsTcpConnectFuture {
        let policy = self.policy.clone();
        Box::pin(async move {
            let socket = configured_tcp_socket(bootstrap, &policy).map_err(map_io_backend_error)?;
            let stream = tokio::time::timeout(timeout, socket.connect(bootstrap))
                .await
                .map_err(|_| DnsBackendError::Timeout)?
                .map_err(map_io_backend_error)?;
            Ok(Box::new(stream) as DnsTcpStream)
        })
    }
}

/// Native socket policy used by one direct DNS backend. The configurator runs
/// after optional source binding and before connect or first send.
#[derive(Clone)]
pub struct DnsNativeSocketPolicy {
    native_sockets: Arc<dyn NativeSocketConfigurator>,
    source_ip: Option<IpAddr>,
}

impl std::fmt::Debug for DnsNativeSocketPolicy {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("DnsNativeSocketPolicy")
            .field("source_ip", &self.source_ip)
            .finish_non_exhaustive()
    }
}

impl DnsNativeSocketPolicy {
    pub fn direct(native_sockets: Arc<dyn NativeSocketConfigurator>) -> Self {
        Self {
            native_sockets,
            source_ip: None,
        }
    }

    pub fn bind_source(
        native_sockets: Arc<dyn NativeSocketConfigurator>,
        source_ip: IpAddr,
    ) -> Self {
        Self {
            native_sockets,
            source_ip: Some(source_ip),
        }
    }

    pub const fn source_ip(&self) -> Option<IpAddr> {
        self.source_ip
    }
}

impl Default for DnsNativeSocketPolicy {
    fn default() -> Self {
        Self::direct(Arc::new(SystemNativeSocketConfigurator))
    }
}

/// Direct literal-IP UDP/TCP/DoT/DoH/DoQ backend factory.
#[derive(Debug, Clone, Default)]
pub struct DirectDnsBackendFactory {
    policy: DnsNativeSocketPolicy,
}

impl DirectDnsBackendFactory {
    pub const fn new(policy: DnsNativeSocketPolicy) -> Self {
        Self { policy }
    }

    /// Build a direct backend after an enclosing registry has proved which
    /// named direct/bind leaf the upstream selected.
    pub fn build_backend_with_policy(
        plan: &CompiledDnsPlan,
        upstream: &CompiledDnsUpstream,
        policy: DnsNativeSocketPolicy,
    ) -> Result<Arc<dyn DnsQueryBackend>, DnsRuntimeError> {
        match upstream.endpoint() {
            DnsUpstreamEndpoint::System => Ok(Arc::new(ExplicitSystemDnsBackend)),
            DnsUpstreamEndpoint::Https { .. } => {
                Ok(Arc::new(DohDnsBackend::compile(plan, upstream, policy)?))
            }
            DnsUpstreamEndpoint::Udp { .. }
            | DnsUpstreamEndpoint::Tcp { .. }
            | DnsUpstreamEndpoint::UdpTcp { .. }
            | DnsUpstreamEndpoint::Tls { .. }
            | DnsUpstreamEndpoint::Quic { .. } => Ok(Arc::new(HickoryDnsBackend::compile(
                plan, upstream, policy,
            )?)),
        }
    }
}

impl DnsBackendFactory for DirectDnsBackendFactory {
    fn build_backend(
        &self,
        plan: &CompiledDnsPlan,
        upstream: &CompiledDnsUpstream,
    ) -> Result<Arc<dyn DnsQueryBackend>, DnsRuntimeError> {
        match upstream.egress() {
            DnsEgressSpec::Direct => {
                Self::build_backend_with_policy(plan, upstream, self.policy.clone())
            }
            DnsEgressSpec::Outbound(outbound) => Err(DnsRuntimeError::MissingEgressConnector {
                upstream: upstream.id().clone(),
                outbound: outbound.clone(),
            }),
        }
    }
}

/// One immutable resolver generation. Plan caches, in-flight indexes, and
/// persistent upstream connections are never shared across generations.
#[derive(Clone)]
pub struct DnsGeneration {
    policy: Arc<CompiledDnsPolicy>,
    plans: Arc<BTreeMap<DnsPlanId, Arc<PlanRuntime>>>,
    fake_dns: Option<Arc<FakeDnsRuntime>>,
}

#[derive(Debug, Clone)]
pub struct DnsRecordResolution {
    message: Message,
    metadata: DnsResolutionMetadata,
    stale: bool,
}

impl DnsRecordResolution {
    pub const fn message(&self) -> &Message {
        &self.message
    }

    pub const fn metadata(&self) -> &DnsResolutionMetadata {
        &self.metadata
    }

    pub const fn is_stale(&self) -> bool {
        self.stale
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DnsRuntimeSnapshot {
    pub generation: u64,
    pub host_overrides: usize,
    pub fake_dns: Option<FakeDnsRuntimeSnapshot>,
    pub plans: Vec<DnsPlanRuntimeSnapshot>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FakeDnsRuntimeSnapshot {
    pub ipv4_pool: Option<IpNet>,
    pub ipv6_pool: Option<IpNet>,
    pub max_entries: usize,
    pub owned_entries: usize,
    pub active_entries: usize,
    pub answers: u64,
    pub recoveries: u64,
    pub expired_recoveries: u64,
    pub unknown_recoveries: u64,
    pub capacity_failures: u64,
}

/// Classification of one TUN destination against the configured FakeDNS pools.
///
/// Unknown and expired synthetic addresses are separate from ordinary
/// destinations so callers can fail closed instead of leaking them to an
/// outbound as if they were real IPs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FakeDnsRecovery {
    NotFake,
    Recovered(DomainName),
    Expired,
    Unknown,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DnsPlanRuntimeSnapshot {
    pub plan: DnsPlanId,
    pub cache_entries: usize,
    pub fresh_cache_entries: usize,
    pub stale_cache_entries: usize,
    pub in_flight: usize,
    pub queries: u64,
    pub fresh_cache_hits: u64,
    pub cache_misses: u64,
    pub coalesced_queries: u64,
    pub refreshes_started: u64,
    pub stale_answers: u64,
    pub cache_evictions: u64,
    pub cache_flushes: u64,
    pub host_answers: u64,
    pub upstream_strategy: DnsUpstreamStrategy,
    pub expected_cidrs: Vec<IpNet>,
    pub upstreams: Vec<DnsUpstreamRuntimeSnapshot>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DnsUpstreamRuntimeSnapshot {
    pub upstream: DnsUpstreamId,
    pub transport: DnsTransport,
    pub bootstrap: Option<SocketAddr>,
    pub egress: DnsEgressSpec,
    pub attempts: u64,
    pub successes: u64,
    pub negative_answers: u64,
    pub failures: u64,
    pub timeouts: u64,
    pub rejected_answers: u64,
    pub canceled_attempts: u64,
    pub total_latency_micros: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DnsQueryExplanation {
    pub generation: u64,
    pub domain: DomainName,
    pub plan: DnsPlanId,
    pub rule: Option<DnsRuleId>,
    pub match_kind: DnsRuleMatchKind,
    pub matched_domain: Option<DomainName>,
    pub explanation: Option<Arc<str>>,
    pub host_addresses: Option<Arc<[IpAddr]>>,
    pub fake_dns: Option<FakeDnsExplanation>,
    pub upstream_strategy: DnsUpstreamStrategy,
    pub expected_cidrs: Vec<IpNet>,
    pub upstreams: Vec<DnsUpstreamDescriptor>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FakeDnsExplanation {
    pub ipv4_pool: Option<IpNet>,
    pub ipv6_pool: Option<IpNet>,
    pub answer_ttl: Duration,
    pub recovery_ttl: Duration,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DnsUpstreamDescriptor {
    pub upstream: DnsUpstreamId,
    pub transport: DnsTransport,
    pub bootstrap: Option<SocketAddr>,
    pub egress: DnsEgressSpec,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DnsCacheFlush {
    pub generation: u64,
    pub plans: usize,
    pub removed_entries: usize,
}

impl std::fmt::Debug for DnsGeneration {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("DnsGeneration")
            .field("generation", &self.policy.generation())
            .field("plans", &self.plans.len())
            .finish()
    }
}

struct FakeDnsRuntime {
    spec: FakeDnsSpec,
    state: Mutex<FakeDnsState>,
    answers: AtomicU64,
    recoveries: AtomicU64,
    expired_recoveries: AtomicU64,
    unknown_recoveries: AtomicU64,
    capacity_failures: AtomicU64,
}

#[derive(Default)]
struct FakeDnsState {
    domains: HashMap<DomainName, FakeDnsLease>,
    addresses: HashMap<IpAddr, DomainName>,
    next_ipv4: u128,
    next_ipv6: u128,
}

struct FakeDnsLease {
    ipv4: Option<Ipv4Addr>,
    ipv6: Option<Ipv6Addr>,
    recover_until: Instant,
}

enum FakeDnsWireAnswer {
    Disabled,
    Address(IpAddr, Duration),
    NoData,
    AtCapacity,
}

impl FakeDnsRuntime {
    fn new(spec: FakeDnsSpec) -> Self {
        Self {
            spec,
            state: Mutex::new(FakeDnsState::default()),
            answers: AtomicU64::new(0),
            recoveries: AtomicU64::new(0),
            expired_recoveries: AtomicU64::new(0),
            unknown_recoveries: AtomicU64::new(0),
            capacity_failures: AtomicU64::new(0),
        }
    }

    fn answer(&self, domain: &DomainName, record_type: RecordType) -> FakeDnsWireAnswer {
        let family = match record_type {
            RecordType::A if self.spec.ipv4_pool.is_some() => NetworkFamily::Ipv4,
            RecordType::AAAA if self.spec.ipv6_pool.is_some() => NetworkFamily::Ipv6,
            RecordType::A | RecordType::AAAA => return FakeDnsWireAnswer::NoData,
            _ => return FakeDnsWireAnswer::Disabled,
        };
        let now = Instant::now();
        let recover_until = now
            .checked_add(self.spec.recovery_ttl)
            .unwrap_or_else(|| now + Duration::from_secs(86_400));
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());

        if let Some(lease) = state.domains.get_mut(domain) {
            lease.recover_until = recover_until;
            let existing = match family {
                NetworkFamily::Ipv4 => lease.ipv4.map(IpAddr::V4),
                NetworkFamily::Ipv6 => lease.ipv6.map(IpAddr::V6),
            };
            if let Some(address) = existing {
                self.answers.fetch_add(1, Ordering::Relaxed);
                return FakeDnsWireAnswer::Address(address, self.spec.answer_ttl);
            }
        } else if state.domains.len() >= self.spec.max_entries {
            self.capacity_failures.fetch_add(1, Ordering::Relaxed);
            return FakeDnsWireAnswer::AtCapacity;
        } else {
            state.domains.insert(
                domain.clone(),
                FakeDnsLease {
                    ipv4: None,
                    ipv6: None,
                    recover_until,
                },
            );
        }

        let address = match family {
            NetworkFamily::Ipv4 => allocate_fake_ipv4(&self.spec, &mut state),
            NetworkFamily::Ipv6 => allocate_fake_ipv6(&self.spec, &mut state),
        };
        let Some(address) = address else {
            self.capacity_failures.fetch_add(1, Ordering::Relaxed);
            return FakeDnsWireAnswer::AtCapacity;
        };
        state.addresses.insert(address, domain.clone());
        let lease = state
            .domains
            .get_mut(domain)
            .expect("FakeDNS domain inserted before address allocation");
        match address {
            IpAddr::V4(address) => lease.ipv4 = Some(address),
            IpAddr::V6(address) => lease.ipv6 = Some(address),
        }
        self.answers.fetch_add(1, Ordering::Relaxed);
        FakeDnsWireAnswer::Address(address, self.spec.answer_ttl)
    }

    fn recover(&self, address: IpAddr) -> FakeDnsRecovery {
        if !fake_dns_contains(&self.spec, address) {
            return FakeDnsRecovery::NotFake;
        }
        let state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        let Some(domain) = state.addresses.get(&address) else {
            self.unknown_recoveries.fetch_add(1, Ordering::Relaxed);
            return FakeDnsRecovery::Unknown;
        };
        let Some(lease) = state.domains.get(domain) else {
            self.unknown_recoveries.fetch_add(1, Ordering::Relaxed);
            return FakeDnsRecovery::Unknown;
        };
        if Instant::now() >= lease.recover_until {
            self.expired_recoveries.fetch_add(1, Ordering::Relaxed);
            return FakeDnsRecovery::Expired;
        }
        self.recoveries.fetch_add(1, Ordering::Relaxed);
        FakeDnsRecovery::Recovered(domain.clone())
    }

    fn snapshot(&self) -> FakeDnsRuntimeSnapshot {
        let now = Instant::now();
        let state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        FakeDnsRuntimeSnapshot {
            ipv4_pool: self.spec.ipv4_pool.map(IpNet::V4),
            ipv6_pool: self.spec.ipv6_pool.map(IpNet::V6),
            max_entries: self.spec.max_entries,
            owned_entries: state.domains.len(),
            active_entries: state
                .domains
                .values()
                .filter(|lease| now < lease.recover_until)
                .count(),
            answers: self.answers.load(Ordering::Relaxed),
            recoveries: self.recoveries.load(Ordering::Relaxed),
            expired_recoveries: self.expired_recoveries.load(Ordering::Relaxed),
            unknown_recoveries: self.unknown_recoveries.load(Ordering::Relaxed),
            capacity_failures: self.capacity_failures.load(Ordering::Relaxed),
        }
    }
}

#[derive(Clone, Copy)]
enum NetworkFamily {
    Ipv4,
    Ipv6,
}

fn allocate_fake_ipv4(spec: &FakeDnsSpec, state: &mut FakeDnsState) -> Option<IpAddr> {
    let pool = spec.ipv4_pool?;
    let base = u32::from(pool.network());
    let addresses = 1_u128 << u32::from(32 - pool.prefix_len());
    let usable = addresses.checked_sub(2)?;
    if state.next_ipv4 >= usable {
        return None;
    }
    let offset = state.next_ipv4.checked_add(1)?;
    state.next_ipv4 = state.next_ipv4.checked_add(1)?;
    let address = u32::try_from(u128::from(base).checked_add(offset)?).ok()?;
    Some(IpAddr::V4(Ipv4Addr::from(address)))
}

fn allocate_fake_ipv6(spec: &FakeDnsSpec, state: &mut FakeDnsState) -> Option<IpAddr> {
    let pool = spec.ipv6_pool?;
    let base = u128::from(pool.network());
    let host_bits = u32::from(128 - pool.prefix_len());
    let addresses = 1_u128.checked_shl(host_bits).unwrap_or(u128::MAX);
    let usable = addresses.saturating_sub(1);
    if state.next_ipv6 >= usable {
        return None;
    }
    let offset = state.next_ipv6.checked_add(1)?;
    state.next_ipv6 = state.next_ipv6.checked_add(1)?;
    Some(IpAddr::V6(Ipv6Addr::from(base.checked_add(offset)?)))
}

fn fake_dns_contains(spec: &FakeDnsSpec, address: IpAddr) -> bool {
    match address {
        IpAddr::V4(address) => spec.ipv4_pool.is_some_and(|pool| pool.contains(&address)),
        IpAddr::V6(address) => spec.ipv6_pool.is_some_and(|pool| pool.contains(&address)),
    }
}

impl DnsGeneration {
    pub fn compile(policy: Arc<CompiledDnsPolicy>) -> Result<Self, DnsRuntimeError> {
        Self::compile_with_factory(policy, &DirectDnsBackendFactory::default())
    }

    pub fn compile_with_native_sockets(
        policy: Arc<CompiledDnsPolicy>,
        native_sockets: Arc<dyn NativeSocketConfigurator>,
    ) -> Result<Self, DnsRuntimeError> {
        Self::compile_with_factory(
            policy,
            &DirectDnsBackendFactory::new(DnsNativeSocketPolicy::direct(native_sockets)),
        )
    }

    pub fn compile_with_factory(
        policy: Arc<CompiledDnsPolicy>,
        factory: &dyn DnsBackendFactory,
    ) -> Result<Self, DnsRuntimeError> {
        Self::compile_selected_with_factory(
            policy,
            factory,
            ProductAdmission::default(),
            None,
            None,
        )
    }

    pub(crate) fn compile_with_factory_and_admission(
        policy: Arc<CompiledDnsPolicy>,
        factory: &dyn DnsBackendFactory,
        admission: ProductAdmission,
    ) -> Result<Self, DnsRuntimeError> {
        Self::compile_selected_with_factory(policy, factory, admission, None, None)
    }

    /// Compile only the direct plans selected by endpoint names that must be
    /// resolved before a managed VPN publishes protected routing. This
    /// deliberately cannot instantiate system or named-outbound DNS.
    #[cfg(any(target_os = "linux", target_os = "windows"))]
    pub(crate) fn compile_prepublication(
        policy: Arc<CompiledDnsPolicy>,
        domains: &[DomainName],
    ) -> Result<Self, DnsRuntimeError> {
        if domains.is_empty() {
            return Err(DnsRuntimeError::PolicyInvariant(
                "pre-publication DNS requires at least one endpoint domain".to_string(),
            ));
        }
        let mut selected = BTreeSet::new();
        let mut network_plans = BTreeSet::new();
        for domain in domains {
            let plan = policy.select(domain).plan().id().clone();
            selected.insert(plan.clone());
            if policy.host(domain).is_none() {
                network_plans.insert(plan);
            }
        }
        for plan_id in &network_plans {
            let plan = policy.plan(plan_id).ok_or_else(|| {
                DnsRuntimeError::PolicyInvariant(format!(
                    "pre-publication DNS lost selected plan {plan_id}"
                ))
            })?;
            for upstream_id in plan.upstreams() {
                let upstream = policy.upstream(upstream_id).ok_or_else(|| {
                    DnsRuntimeError::PolicyInvariant(format!(
                        "pre-publication plan {plan_id} lost upstream {upstream_id}"
                    ))
                })?;
                if matches!(upstream.endpoint(), DnsUpstreamEndpoint::System) {
                    return Err(DnsRuntimeError::PrepublicationSystemDns {
                        plan: plan_id.clone(),
                        upstream: upstream_id.clone(),
                    });
                }
                if let DnsEgressSpec::Outbound(outbound) = upstream.egress() {
                    return Err(DnsRuntimeError::PrepublicationDnsRequiresDirect {
                        plan: plan_id.clone(),
                        upstream: upstream_id.clone(),
                        outbound: outbound.clone(),
                    });
                }
            }
        }
        Self::compile_selected_with_factory(
            policy,
            &DirectDnsBackendFactory::default(),
            ProductAdmission::default(),
            Some(&selected),
            Some(&network_plans),
        )
    }

    fn compile_selected_with_factory(
        policy: Arc<CompiledDnsPolicy>,
        factory: &dyn DnsBackendFactory,
        admission: ProductAdmission,
        selected: Option<&BTreeSet<DnsPlanId>>,
        plans_requiring_backends: Option<&BTreeSet<DnsPlanId>>,
    ) -> Result<Self, DnsRuntimeError> {
        let fake_dns = policy
            .fake_dns()
            .cloned()
            .map(FakeDnsRuntime::new)
            .map(Arc::new);
        let hosts = Arc::new(
            policy
                .hosts()
                .map(|(domain, addresses)| (domain.clone(), addresses.clone()))
                .collect::<BTreeMap<_, _>>(),
        );
        let mut plans = BTreeMap::new();
        for plan in policy
            .plans()
            .filter(|plan| selected.is_none_or(|selected| selected.contains(plan.id())))
        {
            let mut backends = Vec::with_capacity(plan.upstreams().len());
            if plans_requiring_backends.is_none_or(|required| required.contains(plan.id())) {
                for upstream_id in plan.upstreams() {
                    let upstream = policy.upstream(upstream_id).ok_or_else(|| {
                        DnsRuntimeError::PolicyInvariant(format!(
                            "plan {} lost upstream {}",
                            plan.id(),
                            upstream_id
                        ))
                    })?;
                    backends.push(PlanBackend {
                        upstream: upstream.id().clone(),
                        transport: upstream.endpoint().transport(),
                        bootstrap: upstream.endpoint().bootstrap(),
                        egress: upstream.egress().clone(),
                        backend: factory.build_backend(plan, upstream)?,
                        telemetry: UpstreamTelemetry::default(),
                    });
                }
            }
            let runtime = Arc::new(PlanRuntime::new(
                plan,
                backends,
                hosts.clone(),
                admission.clone(),
            ));
            if plans.insert(plan.id().clone(), runtime).is_some() {
                return Err(DnsRuntimeError::PolicyInvariant(format!(
                    "duplicate compiled plan {}",
                    plan.id()
                )));
            }
        }
        Ok(Self {
            policy,
            plans: Arc::new(plans),
            fake_dns,
        })
    }

    pub const fn policy(&self) -> &Arc<CompiledDnsPolicy> {
        &self.policy
    }

    pub fn generation(&self) -> u64 {
        self.policy.generation()
    }

    /// Exact target-resolution bound selected by Product DNS policy.
    ///
    /// Connector and balancer stages use this to keep DNS time separate from
    /// an outbound's own connect timeout. A route-selected plan takes
    /// precedence; otherwise normal exact/suffix/default DNS selection applies.
    pub(crate) fn lookup_timeout(
        &self,
        plan: Option<&DnsPlanId>,
        domain: &DomainName,
    ) -> Result<Duration, DnsRuntimeError> {
        let plan = match plan {
            Some(plan) => self
                .policy
                .plan(plan)
                .ok_or_else(|| DnsRuntimeError::UnknownPlan(plan.clone()))?,
            None => self.policy.select(domain).plan(),
        };
        Ok(plan.limits().lookup_timeout)
    }

    pub fn runtime_snapshot(&self) -> DnsRuntimeSnapshot {
        let now = Instant::now();
        DnsRuntimeSnapshot {
            generation: self.generation(),
            host_overrides: self.policy.hosts().len(),
            fake_dns: self.fake_dns.as_deref().map(FakeDnsRuntime::snapshot),
            plans: self
                .plans
                .values()
                .map(|runtime| runtime.snapshot(now))
                .collect(),
        }
    }

    /// Explain split-policy and upstream routing without performing a query.
    pub fn explain(&self, domain: &DomainName) -> DnsQueryExplanation {
        let selection = self.policy.select(domain);
        let runtime = self
            .plans
            .get(selection.plan().id())
            .expect("compiled DNS plan runtime");
        let host_addresses = self.policy.host(domain).cloned();
        let fake_dns = self.policy.fake_dns().map(|spec| FakeDnsExplanation {
            ipv4_pool: spec.ipv4_pool.map(IpNet::V4),
            ipv6_pool: spec.ipv6_pool.map(IpNet::V6),
            answer_ttl: spec.answer_ttl,
            recovery_ttl: spec.recovery_ttl,
        });
        DnsQueryExplanation {
            generation: selection.generation(),
            domain: domain.clone(),
            plan: selection.plan().id().clone(),
            rule: selection.rule_id().cloned(),
            match_kind: selection.match_kind(),
            matched_domain: selection.matched_domain().cloned(),
            explanation: selection.explanation().map(Arc::from),
            host_addresses: host_addresses.clone(),
            fake_dns,
            upstream_strategy: selection.plan().upstream_strategy(),
            expected_cidrs: selection.plan().expected_cidrs().to_vec(),
            upstreams: if host_addresses.is_some() {
                Vec::new()
            } else {
                runtime
                    .backends
                    .iter()
                    .map(PlanBackend::descriptor)
                    .collect()
            },
        }
    }

    pub fn flush_cache(&self, plan: Option<&DnsPlanId>) -> Result<DnsCacheFlush, DnsRuntimeError> {
        let (plans, removed_entries) = match plan {
            Some(plan) => {
                let runtime = self
                    .plans
                    .get(plan)
                    .ok_or_else(|| DnsRuntimeError::UnknownPlan(plan.clone()))?;
                (1, runtime.flush_cache())
            }
            None => (
                self.plans.len(),
                self.plans
                    .values()
                    .map(|runtime| runtime.flush_cache())
                    .sum(),
            ),
        };
        Ok(DnsCacheFlush {
            generation: self.generation(),
            plans,
            removed_entries,
        })
    }

    /// Resolve according to exact/suffix/default split-DNS selection.
    pub async fn resolve(&self, domain: &DomainName) -> Result<DnsResolution, DnsRuntimeError> {
        let selection = self.policy.select(domain);
        let addresses = self.resolve_in_plan(selection.plan().id(), domain).await?;
        Ok(DnsResolution {
            addresses,
            metadata: DnsResolutionMetadata {
                generation: selection.generation(),
                plan: selection.plan().id().clone(),
                rule: selection.rule_id().cloned(),
                match_kind: selection.match_kind(),
                matched_domain: selection.matched_domain().cloned(),
                explanation: selection.explanation().map(Arc::from),
            },
        })
    }

    pub fn recover_fake_dns(&self, address: IpAddr) -> FakeDnsRecovery {
        self.fake_dns
            .as_deref()
            .map_or(FakeDnsRecovery::NotFake, |fake_dns| {
                fake_dns.recover(address)
            })
    }

    /// Query one arbitrary IN-class record type through split-DNS selection.
    /// This is the shared record engine used by local wire capture; dial-time
    /// A/AAAA resolution uses the same plan caches and in-flight index.
    pub async fn query_record(
        &self,
        domain: &DomainName,
        record_type: RecordType,
    ) -> Result<DnsRecordResolution, DnsRuntimeError> {
        let selection = self.policy.select(domain);
        let resolved = self
            .query_record_in_plan(selection.plan().id(), domain, record_type)
            .await?;
        Ok(DnsRecordResolution {
            message: render_resolved_message(&resolved, None),
            metadata: DnsResolutionMetadata {
                generation: selection.generation(),
                plan: selection.plan().id().clone(),
                rule: selection.rule_id().cloned(),
                match_kind: selection.match_kind(),
                matched_domain: selection.matched_domain().cloned(),
                explanation: selection.explanation().map(Arc::from),
            },
            stale: resolved.stale,
        })
    }

    async fn query_record_in_plan(
        &self,
        plan: &DnsPlanId,
        domain: &DomainName,
        record_type: RecordType,
    ) -> Result<ResolvedRecord, DnsRuntimeError> {
        let runtime = self
            .plans
            .get(plan)
            .ok_or_else(|| DnsRuntimeError::UnknownPlan(plan.clone()))?;
        runtime
            .query(DnsQuestion::new(domain.clone(), record_type))
            .await
    }

    /// Resolve with an explicitly selected plan, for a routing decision that
    /// already carries a `DnsPlanId`.
    pub async fn resolve_in_plan(
        &self,
        plan: &DnsPlanId,
        domain: &DomainName,
    ) -> Result<Arc<[IpAddr]>, DnsRuntimeError> {
        let runtime = self
            .plans
            .get(plan)
            .ok_or_else(|| DnsRuntimeError::UnknownPlan(plan.clone()))?;
        runtime.resolve(domain.clone()).await
    }

    pub async fn resolve_socket_addrs(
        &self,
        host: &str,
        port: u16,
    ) -> Result<Vec<SocketAddr>, DnsRuntimeError> {
        self.resolve_socket_addrs_for_plan(None, host, port).await
    }

    pub async fn resolve_socket_addrs_for_plan(
        &self,
        plan: Option<&DnsPlanId>,
        host: &str,
        port: u16,
    ) -> Result<Vec<SocketAddr>, DnsRuntimeError> {
        if port == 0 {
            return Err(DnsRuntimeError::InvalidPort);
        }
        if let Ok(address) = host.parse::<IpAddr>() {
            return Ok(vec![SocketAddr::new(address, port)]);
        }
        let domain = DomainName::parse(host).map_err(|error| DnsRuntimeError::InvalidDomain {
            domain: host.to_string(),
            message: error.to_string(),
        })?;
        let addresses = match plan {
            Some(plan) => self.resolve_in_plan(plan, &domain).await?,
            None => self.resolve(&domain).await?.addresses,
        };
        Ok(addresses
            .iter()
            .copied()
            .map(|address| SocketAddr::new(address, port))
            .collect())
    }

    /// Answers one bounded IN-class DNS wire query through this immutable
    /// split-DNS generation. Standard record sections are retained, while
    /// transfer queries, malformed messages, and oversized responses fail
    /// closed.
    ///
    /// This is the local resolver boundary used by managed VPN DNS capture.
    /// It never routes the request as an ordinary port-53 Product flow, so a
    /// captured query cannot recursively enter the VPN. The caller-provided
    /// TTL is a publication cap; upstream/cache TTLs are never extended.
    pub async fn answer_wire_query(
        &self,
        request_bytes: &[u8],
        answer_ttl: Duration,
        response_limit: usize,
    ) -> Result<Bytes, DnsWireError> {
        if request_bytes.len() > MAX_DNS_WIRE_MESSAGE_BYTES {
            return Err(DnsWireError::RequestTooLarge {
                actual: request_bytes.len(),
                maximum: MAX_DNS_WIRE_MESSAGE_BYTES,
            });
        }
        let request = Message::from_vec(request_bytes)
            .map_err(|error| DnsWireError::Decode(error.to_string()))?;
        let mut response = dns_wire_response_for_request(&request, response_limit);

        if request.metadata.message_type != MessageType::Query
            || request.metadata.op_code != OpCode::Query
            || request.queries.len() != 1
        {
            response.metadata.response_code = ResponseCode::FormErr;
            return encode_dns_wire_response(response, response_limit);
        }

        let query = request
            .queries
            .first()
            .expect("one DNS query was checked above");
        response.add_query(query.clone());
        if query.query_class() != DNSClass::IN {
            response.metadata.response_code = ResponseCode::NotImp;
            return encode_dns_wire_response(response, response_limit);
        }
        if matches!(query.query_type(), RecordType::AXFR | RecordType::IXFR) {
            response.metadata.response_code = ResponseCode::NotImp;
            return encode_dns_wire_response(response, response_limit);
        }

        let domain_text = query.name().to_utf8();
        let domain_text = domain_text.strip_suffix('.').unwrap_or(&domain_text);
        let domain = match DomainName::parse(domain_text) {
            Ok(domain) => domain,
            Err(_) => {
                response.metadata.response_code = ResponseCode::FormErr;
                return encode_dns_wire_response(response, response_limit);
            }
        };
        if self.policy.host(&domain).is_none()
            && let Some(fake_dns) = self.fake_dns.as_deref()
        {
            match fake_dns.answer(&domain, query.query_type()) {
                FakeDnsWireAnswer::Address(address, fake_ttl) => {
                    let data = match address {
                        IpAddr::V4(address) => RData::A(A(address)),
                        IpAddr::V6(address) => RData::AAAA(AAAA(address)),
                    };
                    response.add_answer(Record::from_rdata(
                        query.name().clone(),
                        dns_wire_ttl(fake_ttl.min(answer_ttl)),
                        data,
                    ));
                    return encode_dns_wire_response(response, response_limit);
                }
                FakeDnsWireAnswer::NoData => {
                    return encode_dns_wire_response(response, response_limit);
                }
                FakeDnsWireAnswer::AtCapacity => {
                    response.metadata.response_code = ResponseCode::ServFail;
                    return encode_dns_wire_response(response, response_limit);
                }
                FakeDnsWireAnswer::Disabled => {}
            }
        }
        match self
            .query_record_in_plan(
                self.policy.select(&domain).plan().id(),
                &domain,
                query.query_type(),
            )
            .await
        {
            Ok(answer) => {
                let answer = render_resolved_message(&answer, Some(answer_ttl));
                response.metadata.authoritative = answer.metadata.authoritative;
                response.metadata.authentic_data = answer.metadata.authentic_data;
                response.metadata.response_code = answer.metadata.response_code;
                response.add_answers(answer.answers);
                response.add_authorities(answer.authorities);
                response.add_additionals(answer.additionals);
            }
            Err(_) => {
                response.metadata.response_code = ResponseCode::ServFail;
            }
        }
        encode_dns_wire_response(response, response_limit)
    }

    #[cfg(test)]
    pub(crate) fn from_test_answers(answers: HashMap<String, Vec<IpAddr>>) -> Self {
        use crate::product::{DnsPlanSpec, DnsPolicySpec, DnsUpstreamSpec};

        let upstream = DnsUpstreamId::parse("test-static").expect("static upstream ID");
        let plan = DnsPlanId::parse("test-default").expect("static plan ID");
        let policy = Arc::new(
            CompiledDnsPolicy::compile(
                1,
                DnsPolicySpec {
                    upstreams: vec![DnsUpstreamSpec::direct(
                        upstream.clone(),
                        DnsUpstreamEndpoint::System,
                    )],
                    outbound_capabilities: Vec::new(),
                    plans: vec![DnsPlanSpec::new(plan.clone(), vec![upstream.clone()])],
                    rules: Vec::new(),
                    hosts: Vec::new(),
                    fake_dns: None,
                    default_plan: plan,
                },
            )
            .expect("static test DNS policy"),
        );
        let answers = answers
            .into_iter()
            .map(|(domain, addresses)| {
                (
                    DomainName::parse(&domain).expect("static test DNS domain"),
                    addresses,
                )
            })
            .collect();
        Self::compile_with_factory(
            policy,
            &StaticTestBackendFactory {
                upstream,
                answers: Arc::new(answers),
            },
        )
        .expect("static test DNS generation")
    }
}

#[derive(Debug, Clone)]
pub struct DnsResolution {
    addresses: Arc<[IpAddr]>,
    metadata: DnsResolutionMetadata,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DnsWireError {
    RequestTooLarge { actual: usize, maximum: usize },
    Decode(String),
    Encode(String),
    ResponseLimitTooSmall { actual: usize, limit: usize },
}

impl std::fmt::Display for DnsWireError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::RequestTooLarge { actual, maximum } => write!(
                formatter,
                "DNS request is {actual} bytes; maximum is {maximum} bytes"
            ),
            Self::Decode(message) => write!(formatter, "invalid DNS request: {message}"),
            Self::Encode(message) => write!(formatter, "failed to encode DNS response: {message}"),
            Self::ResponseLimitTooSmall { actual, limit } => write!(
                formatter,
                "truncated DNS response is {actual} bytes; response limit is {limit} bytes"
            ),
        }
    }
}

impl std::error::Error for DnsWireError {}

fn dns_wire_response_for_request(request: &Message, response_limit: usize) -> Message {
    let mut response = Message::response(request.metadata.id, request.metadata.op_code);
    response.metadata.recursion_desired = request.metadata.recursion_desired;
    response.metadata.recursion_available = true;
    response.metadata.checking_disabled = request.metadata.checking_disabled;
    if let Some(request_edns) = request.edns.as_ref() {
        let mut edns = Edns::new();
        let response_limit = u16::try_from(response_limit).unwrap_or(u16::MAX);
        edns.set_max_payload(request_edns.max_payload().min(response_limit));
        edns.set_dnssec_ok(request_edns.flags().dnssec_ok);
        response.set_edns(edns);
    }
    response
}

fn encode_dns_wire_response(
    response: Message,
    response_limit: usize,
) -> Result<Bytes, DnsWireError> {
    let response_limit = response_limit.min(MAX_DNS_WIRE_MESSAGE_BYTES);
    let encoded = response
        .to_vec()
        .map_err(|error| DnsWireError::Encode(error.to_string()))?;
    if encoded.len() <= response_limit {
        return Ok(Bytes::from(encoded));
    }
    let encoded = response
        .truncate()
        .to_vec()
        .map_err(|error| DnsWireError::Encode(error.to_string()))?;
    if encoded.len() > response_limit {
        return Err(DnsWireError::ResponseLimitTooSmall {
            actual: encoded.len(),
            limit: response_limit,
        });
    }
    Ok(Bytes::from(encoded))
}

fn dns_wire_ttl(ttl: Duration) -> u32 {
    u32::try_from(ttl.as_secs()).unwrap_or(u32::MAX).max(1)
}

impl DnsResolution {
    pub const fn addresses(&self) -> &Arc<[IpAddr]> {
        &self.addresses
    }

    pub const fn metadata(&self) -> &DnsResolutionMetadata {
        &self.metadata
    }
}

#[derive(Debug, Clone)]
pub struct DnsResolutionMetadata {
    generation: u64,
    plan: DnsPlanId,
    rule: Option<DnsRuleId>,
    match_kind: DnsRuleMatchKind,
    matched_domain: Option<DomainName>,
    explanation: Option<Arc<str>>,
}

impl DnsResolutionMetadata {
    pub const fn generation(&self) -> u64 {
        self.generation
    }

    pub const fn plan(&self) -> &DnsPlanId {
        &self.plan
    }

    pub const fn rule(&self) -> Option<&DnsRuleId> {
        self.rule.as_ref()
    }

    pub const fn match_kind(&self) -> DnsRuleMatchKind {
        self.match_kind
    }

    pub const fn matched_domain(&self) -> Option<&DomainName> {
        self.matched_domain.as_ref()
    }

    pub fn explanation(&self) -> Option<&str> {
        self.explanation.as_deref()
    }
}

struct PlanBackend {
    upstream: DnsUpstreamId,
    transport: DnsTransport,
    bootstrap: Option<SocketAddr>,
    egress: DnsEgressSpec,
    backend: Arc<dyn DnsQueryBackend>,
    telemetry: UpstreamTelemetry,
}

impl PlanBackend {
    fn descriptor(&self) -> DnsUpstreamDescriptor {
        DnsUpstreamDescriptor {
            upstream: self.upstream.clone(),
            transport: self.transport,
            bootstrap: self.bootstrap,
            egress: self.egress.clone(),
        }
    }
}

type UpstreamQueryResult = (
    Result<ResolvedRecord, DnsRuntimeError>,
    Option<(Arc<CachedDnsResponse>, Duration)>,
);

enum UpstreamAttempt {
    Answer(Arc<CachedDnsResponse>, Duration),
    Failed {
        upstream: DnsUpstreamId,
        message: String,
    },
    TooManyAnswers(usize),
    Deadline,
}

fn successful_upstream_result(
    answer: Arc<CachedDnsResponse>,
    ttl: Duration,
) -> UpstreamQueryResult {
    (
        Ok(ResolvedRecord::fresh(answer.clone())),
        Some((answer, ttl)),
    )
}

struct PlanRuntime {
    id: DnsPlanId,
    strategy: DnsIpStrategy,
    upstream_strategy: DnsUpstreamStrategy,
    expected_cidrs: Arc<[IpNet]>,
    limits: DnsPlanLimits,
    backends: Vec<PlanBackend>,
    hosts: Arc<BTreeMap<DomainName, Arc<[IpAddr]>>>,
    admission: ProductAdmission,
    permits: Arc<Semaphore>,
    state: Mutex<PlanState>,
    telemetry: PlanTelemetry,
}

impl PlanRuntime {
    fn new(
        plan: &CompiledDnsPlan,
        backends: Vec<PlanBackend>,
        hosts: Arc<BTreeMap<DomainName, Arc<[IpAddr]>>>,
        admission: ProductAdmission,
    ) -> Self {
        Self {
            id: plan.id().clone(),
            strategy: plan.ip_strategy(),
            upstream_strategy: plan.upstream_strategy(),
            expected_cidrs: Arc::from(plan.expected_cidrs()),
            limits: plan.limits(),
            backends,
            hosts,
            admission,
            permits: Arc::new(Semaphore::new(plan.limits().max_inflight)),
            state: Mutex::new(PlanState::default()),
            telemetry: PlanTelemetry::default(),
        }
    }

    async fn resolve(
        self: &Arc<Self>,
        domain: DomainName,
    ) -> Result<Arc<[IpAddr]>, DnsRuntimeError> {
        let query = |record_type| self.query(DnsQuestion::new(domain.clone(), record_type));
        let results = match self.strategy {
            DnsIpStrategy::Ipv4Only => vec![query(RecordType::A).await],
            DnsIpStrategy::Ipv6Only => vec![query(RecordType::AAAA).await],
            DnsIpStrategy::Ipv4ThenIpv6 => {
                let primary = query(RecordType::A).await;
                if record_resolution_has_addresses(&primary, RecordType::A)
                    || record_resolution_is_nx_domain(&primary)
                {
                    vec![primary]
                } else {
                    vec![primary, query(RecordType::AAAA).await]
                }
            }
            DnsIpStrategy::Ipv6ThenIpv4 => {
                let primary = query(RecordType::AAAA).await;
                if record_resolution_has_addresses(&primary, RecordType::AAAA)
                    || record_resolution_is_nx_domain(&primary)
                {
                    vec![primary]
                } else {
                    vec![primary, query(RecordType::A).await]
                }
            }
            DnsIpStrategy::Ipv4AndIpv6 | DnsIpStrategy::Ipv6AndIpv4
                if self.limits.max_inflight > 1 =>
            {
                let (ipv4, ipv6) = tokio::join!(query(RecordType::A), query(RecordType::AAAA));
                if self.strategy == DnsIpStrategy::Ipv4AndIpv6 {
                    vec![ipv4, ipv6]
                } else {
                    vec![ipv6, ipv4]
                }
            }
            DnsIpStrategy::Ipv4AndIpv6 => {
                vec![query(RecordType::A).await, query(RecordType::AAAA).await]
            }
            DnsIpStrategy::Ipv6AndIpv4 => {
                vec![query(RecordType::AAAA).await, query(RecordType::A).await]
            }
        };
        merge_address_record_results(&self.id, &domain, self.limits.max_answers, results)
    }

    async fn query(
        self: &Arc<Self>,
        question: DnsQuestion,
    ) -> Result<ResolvedRecord, DnsRuntimeError> {
        self.telemetry.queries.fetch_add(1, Ordering::Relaxed);
        if let Some(addresses) = self.hosts.get(&question.domain) {
            self.telemetry.host_answers.fetch_add(1, Ordering::Relaxed);
            return Ok(ResolvedRecord::fresh(Arc::new(hosts_response(
                &question,
                addresses,
                self.limits.positive_ttl_cap,
            ))));
        }
        let mut launch = None;
        let disposition = {
            let mut state = self.state.lock().expect("DNS plan state lock");
            let now = Instant::now();
            match state.cache.lookup(&question, now) {
                Some(hit) if !hit.stale => {
                    self.telemetry
                        .fresh_cache_hits
                        .fetch_add(1, Ordering::Relaxed);
                    if hit.refresh_due
                        && !state.in_flight.contains_key(&question)
                        && let Ok((permit, product_work)) = self.try_launch_permits()
                    {
                        let flight = Arc::new(LookupFlight::new());
                        state.in_flight.insert(question.clone(), flight.clone());
                        launch = Some(LookupLaunch {
                            flight,
                            permit,
                            product_work,
                            cache_epoch: state.cache_epoch,
                            stale: Some(hit.clone()),
                        });
                        self.telemetry
                            .refreshes_started
                            .fetch_add(1, Ordering::Relaxed);
                    }
                    QueryDisposition::Return(hit.resolved())
                }
                cached => {
                    self.telemetry.cache_misses.fetch_add(1, Ordering::Relaxed);
                    if let Some(flight) = state.in_flight.get(&question) {
                        self.telemetry
                            .coalesced_queries
                            .fetch_add(1, Ordering::Relaxed);
                        QueryDisposition::Wait(flight.clone())
                    } else {
                        match self.try_launch_permits() {
                            Ok((permit, product_work)) => {
                                let flight = Arc::new(LookupFlight::new());
                                state.in_flight.insert(question.clone(), flight.clone());
                                launch = Some(LookupLaunch {
                                    flight: flight.clone(),
                                    permit,
                                    product_work,
                                    cache_epoch: state.cache_epoch,
                                    stale: cached,
                                });
                                QueryDisposition::Wait(flight)
                            }
                            Err(error) => {
                                if let Some(stale) = cached {
                                    self.telemetry.stale_answers.fetch_add(1, Ordering::Relaxed);
                                    QueryDisposition::Return(stale.resolved())
                                } else {
                                    return Err(error);
                                }
                            }
                        }
                    }
                }
            }
        };

        if let Some(launch) = launch {
            let runtime = self.clone();
            let query_question = question;
            tokio::spawn(async move {
                runtime.perform_query(query_question, launch).await;
            });
        }
        match disposition {
            QueryDisposition::Return(answer) => Ok(answer),
            QueryDisposition::Wait(flight) => {
                let result = flight.wait().await;
                if result.as_ref().is_ok_and(|answer| answer.stale) {
                    self.telemetry.stale_answers.fetch_add(1, Ordering::Relaxed);
                }
                result
            }
        }
    }

    fn try_launch_permits(
        &self,
    ) -> Result<(OwnedSemaphorePermit, ProductDnsWork), DnsRuntimeError> {
        let permit =
            self.permits
                .clone()
                .try_acquire_owned()
                .map_err(|_| DnsRuntimeError::AtCapacity {
                    plan: self.id.clone(),
                    limit: self.limits.max_inflight,
                })?;
        let product_work = self.admission.try_admit_dns_work().map_err(|error| {
            DnsRuntimeError::ProductAtCapacity {
                rejection: error.rejection(),
            }
        })?;
        Ok((permit, product_work))
    }

    async fn perform_query(self: Arc<Self>, question: DnsQuestion, launch: LookupLaunch) {
        let LookupLaunch {
            flight,
            permit: _permit,
            product_work: _product_work,
            cache_epoch,
            stale,
        } = launch;
        let deadline = tokio::time::Instant::now() + self.limits.lookup_timeout;
        let (mut result, cached) = self.query_upstreams(&question, deadline).await;
        if result.as_ref().is_err_and(stale_fallback_allowed)
            && let Some(stale) = stale
        {
            result = Ok(stale.stale_resolved());
        }
        let now = Instant::now();
        let mut state = self.state.lock().expect("DNS plan state lock");
        if state.cache_epoch == cache_epoch
            && let Some((answer, ttl)) = cached
        {
            let evicted = state.cache.insert(
                question.clone(),
                answer,
                ttl,
                self.limits.cache_capacity,
                self.limits.stale_if_error,
                self.limits.prefetch_max,
                now,
            );
            self.telemetry
                .cache_evictions
                .fetch_add(evicted as u64, Ordering::Relaxed);
        }
        flight.finish(result);
        if state
            .in_flight
            .get(&question)
            .is_some_and(|current| Arc::ptr_eq(current, &flight))
        {
            state.in_flight.remove(&question);
        }
    }

    async fn query_upstreams(
        &self,
        question: &DnsQuestion,
        deadline: tokio::time::Instant,
    ) -> UpstreamQueryResult {
        match self.upstream_strategy {
            DnsUpstreamStrategy::Ordered => self.query_upstreams_ordered(question, deadline).await,
            DnsUpstreamStrategy::Race { fallback_delay } => {
                self.query_upstreams_race(question, deadline, fallback_delay)
                    .await
            }
        }
    }

    async fn query_upstreams_ordered(
        &self,
        question: &DnsQuestion,
        deadline: tokio::time::Instant,
    ) -> UpstreamQueryResult {
        let mut last_failure = None;
        for index in 0..self.backends.len() {
            let (_, outcome) = self.query_upstream(index, question, deadline).await;
            match outcome {
                UpstreamAttempt::Answer(answer, ttl) => {
                    return successful_upstream_result(answer, ttl);
                }
                UpstreamAttempt::Failed { upstream, message } => {
                    last_failure = Some((upstream, message));
                }
                UpstreamAttempt::TooManyAnswers(count) => {
                    return self.too_many_answers(question, count);
                }
                UpstreamAttempt::Deadline => return self.query_timeout(question),
            }
            if index + 1 < self.backends.len() && tokio::time::Instant::now() >= deadline {
                return self.query_timeout(question);
            }
        }
        self.all_upstreams_failed(question, last_failure)
    }

    async fn query_upstreams_race(
        &self,
        question: &DnsQuestion,
        deadline: tokio::time::Instant,
        fallback_delay: Duration,
    ) -> UpstreamQueryResult {
        if self.backends.len() < 2 {
            return self.query_upstreams_ordered(question, deadline).await;
        }

        let mut attempts = FuturesUnordered::new();
        let mut active = BTreeSet::new();
        let mut next = 0_usize;
        let mut last_failure = None;
        attempts.push(self.query_upstream(next, question, deadline));
        active.insert(next);
        next += 1;
        let mut next_launch = tokio::time::Instant::now() + fallback_delay;

        loop {
            if attempts.is_empty() {
                if next >= self.backends.len() {
                    return self.all_upstreams_failed(question, last_failure);
                }
                attempts.push(self.query_upstream(next, question, deadline));
                active.insert(next);
                next += 1;
                next_launch = tokio::time::Instant::now() + fallback_delay;
            }

            let wake_at = if next < self.backends.len() {
                next_launch.min(deadline)
            } else {
                deadline
            };
            tokio::select! {
                biased;
                outcome = attempts.next() => {
                    let Some((index, outcome)) = outcome else {
                        continue;
                    };
                    active.remove(&index);
                    match outcome {
                        UpstreamAttempt::Answer(answer, ttl) => {
                            self.cancel_race_attempts(&active);
                            return successful_upstream_result(answer, ttl);
                        }
                        UpstreamAttempt::Failed { upstream, message } => {
                            last_failure = Some((upstream, message));
                        }
                        UpstreamAttempt::TooManyAnswers(count) => {
                            self.cancel_race_attempts(&active);
                            return self.too_many_answers(question, count);
                        }
                        UpstreamAttempt::Deadline => {
                            self.cancel_race_attempts(&active);
                            return self.query_timeout(question);
                        }
                    }
                }
                _ = tokio::time::sleep_until(wake_at) => {
                    if tokio::time::Instant::now() >= deadline {
                        self.cancel_race_attempts(&active);
                        return self.query_timeout(question);
                    }
                    attempts.push(self.query_upstream(next, question, deadline));
                    active.insert(next);
                    next += 1;
                    next_launch = tokio::time::Instant::now() + fallback_delay;
                }
            }
        }
    }

    async fn query_upstream(
        &self,
        index: usize,
        question: &DnsQuestion,
        deadline: tokio::time::Instant,
    ) -> (usize, UpstreamAttempt) {
        let upstream = &self.backends[index];
        upstream.telemetry.attempts.fetch_add(1, Ordering::Relaxed);
        let started = Instant::now();
        let lookup =
            tokio::time::timeout_at(deadline, upstream.backend.query(question.clone())).await;
        let outcome = match lookup {
            Err(_) => {
                upstream.telemetry.timeouts.fetch_add(1, Ordering::Relaxed);
                UpstreamAttempt::Deadline
            }
            Ok(Err(DnsBackendError::Timeout)) => {
                upstream.telemetry.timeouts.fetch_add(1, Ordering::Relaxed);
                UpstreamAttempt::Failed {
                    upstream: upstream.upstream.clone(),
                    message: "upstream query timed out".to_string(),
                }
            }
            Ok(Err(DnsBackendError::Failed(message))) => {
                upstream.telemetry.failures.fetch_add(1, Ordering::Relaxed);
                UpstreamAttempt::Failed {
                    upstream: upstream.upstream.clone(),
                    message: bounded_backend_message(message),
                }
            }
            Ok(Err(DnsBackendError::NoRecords { ttl })) => {
                upstream
                    .telemetry
                    .negative_answers
                    .fetch_add(1, Ordering::Relaxed);
                let ttl = effective_ttl(ttl, NEGATIVE_FALLBACK_TTL, self.limits.negative_ttl_cap);
                UpstreamAttempt::Answer(
                    Arc::new(negative_cached_response(question, ResponseCode::NXDomain)),
                    ttl,
                )
            }
            Ok(Ok(answer)) => match normalize_backend_response(question, answer, self.limits) {
                Ok((answer, ttl)) => {
                    if let Err(message) =
                        validate_expected_addresses(question, &answer, &self.expected_cidrs)
                    {
                        upstream
                            .telemetry
                            .rejected_answers
                            .fetch_add(1, Ordering::Relaxed);
                        upstream.telemetry.failures.fetch_add(1, Ordering::Relaxed);
                        UpstreamAttempt::Failed {
                            upstream: upstream.upstream.clone(),
                            message,
                        }
                    } else {
                        upstream.telemetry.successes.fetch_add(1, Ordering::Relaxed);
                        upstream.telemetry.total_latency_micros.fetch_add(
                            started.elapsed().as_micros().min(u64::MAX as u128) as u64,
                            Ordering::Relaxed,
                        );
                        if !answer.positive {
                            upstream
                                .telemetry
                                .negative_answers
                                .fetch_add(1, Ordering::Relaxed);
                        }
                        UpstreamAttempt::Answer(answer, ttl)
                    }
                }
                Err(DnsResponseValidationError::TooManyAnswers(count)) => {
                    upstream.telemetry.failures.fetch_add(1, Ordering::Relaxed);
                    UpstreamAttempt::TooManyAnswers(count)
                }
                Err(DnsResponseValidationError::Failed(message)) => {
                    upstream.telemetry.failures.fetch_add(1, Ordering::Relaxed);
                    UpstreamAttempt::Failed {
                        upstream: upstream.upstream.clone(),
                        message: bounded_backend_message(message),
                    }
                }
            },
        };
        (index, outcome)
    }

    fn cancel_race_attempts(&self, active: &BTreeSet<usize>) {
        for index in active {
            self.backends[*index]
                .telemetry
                .canceled_attempts
                .fetch_add(1, Ordering::Relaxed);
        }
    }

    fn too_many_answers(&self, question: &DnsQuestion, count: usize) -> UpstreamQueryResult {
        (
            Err(DnsRuntimeError::TooManyAnswers {
                plan: self.id.clone(),
                domain: question.domain.clone(),
                count,
                maximum: self.limits.max_answers,
            }),
            None,
        )
    }

    fn query_timeout(&self, question: &DnsQuestion) -> UpstreamQueryResult {
        (
            Err(DnsRuntimeError::Timeout {
                plan: self.id.clone(),
                domain: question.domain.clone(),
            }),
            None,
        )
    }

    fn all_upstreams_failed(
        &self,
        question: &DnsQuestion,
        last_failure: Option<(DnsUpstreamId, String)>,
    ) -> UpstreamQueryResult {
        let (upstream, message) = last_failure.unwrap_or_else(|| {
            (
                DnsUpstreamId::parse("unavailable").expect("static DNS upstream ID"),
                "DNS plan has no usable backend".to_string(),
            )
        });
        (
            Err(DnsRuntimeError::AllUpstreamsFailed {
                plan: self.id.clone(),
                domain: question.domain.clone(),
                last_upstream: upstream,
                message,
            }),
            None,
        )
    }

    fn snapshot(&self, now: Instant) -> DnsPlanRuntimeSnapshot {
        let mut state = self.state.lock().expect("DNS plan state lock");
        let (fresh_cache_entries, stale_cache_entries) = state.cache.counts(now);
        DnsPlanRuntimeSnapshot {
            plan: self.id.clone(),
            cache_entries: fresh_cache_entries + stale_cache_entries,
            fresh_cache_entries,
            stale_cache_entries,
            in_flight: state.in_flight.len(),
            queries: self.telemetry.queries.load(Ordering::Relaxed),
            fresh_cache_hits: self.telemetry.fresh_cache_hits.load(Ordering::Relaxed),
            cache_misses: self.telemetry.cache_misses.load(Ordering::Relaxed),
            coalesced_queries: self.telemetry.coalesced_queries.load(Ordering::Relaxed),
            refreshes_started: self.telemetry.refreshes_started.load(Ordering::Relaxed),
            stale_answers: self.telemetry.stale_answers.load(Ordering::Relaxed),
            cache_evictions: self.telemetry.cache_evictions.load(Ordering::Relaxed),
            cache_flushes: self.telemetry.cache_flushes.load(Ordering::Relaxed),
            host_answers: self.telemetry.host_answers.load(Ordering::Relaxed),
            upstream_strategy: self.upstream_strategy,
            expected_cidrs: self.expected_cidrs.to_vec(),
            upstreams: self
                .backends
                .iter()
                .map(|backend| backend.telemetry.snapshot(backend.descriptor()))
                .collect(),
        }
    }

    fn flush_cache(&self) -> usize {
        let mut state = self.state.lock().expect("DNS plan state lock");
        state.cache_epoch = state.cache_epoch.wrapping_add(1);
        let removed = state.cache.clear();
        self.telemetry.cache_flushes.fetch_add(1, Ordering::Relaxed);
        removed
    }
}

#[derive(Default)]
struct PlanState {
    cache: DeterministicCache,
    in_flight: HashMap<DnsQuestion, Arc<LookupFlight>>,
    cache_epoch: u64,
}

#[derive(Default)]
struct DeterministicCache {
    entries: HashMap<DnsQuestion, CacheEntry>,
    insertion_order: BTreeMap<u64, DnsQuestion>,
    next_sequence: u64,
}

struct CacheEntry {
    answer: Arc<CachedDnsResponse>,
    inserted_at: Instant,
    refresh_at: Instant,
    fresh_until: Instant,
    stale_until: Instant,
    sequence: u64,
}

#[derive(Clone)]
struct CacheHit {
    answer: Arc<CachedDnsResponse>,
    age: Duration,
    stale: bool,
    refresh_due: bool,
}

impl CacheHit {
    fn resolved(&self) -> ResolvedRecord {
        ResolvedRecord {
            answer: self.answer.clone(),
            age: self.age,
            stale: self.stale,
        }
    }

    fn stale_resolved(&self) -> ResolvedRecord {
        ResolvedRecord {
            answer: self.answer.clone(),
            age: self.age,
            stale: true,
        }
    }
}

struct CachedDnsResponse {
    message: Message,
    positive: bool,
}

#[derive(Clone)]
struct ResolvedRecord {
    answer: Arc<CachedDnsResponse>,
    age: Duration,
    stale: bool,
}

impl ResolvedRecord {
    fn fresh(answer: Arc<CachedDnsResponse>) -> Self {
        Self {
            answer,
            age: Duration::ZERO,
            stale: false,
        }
    }
}

struct LookupLaunch {
    flight: Arc<LookupFlight>,
    permit: OwnedSemaphorePermit,
    product_work: ProductDnsWork,
    cache_epoch: u64,
    stale: Option<CacheHit>,
}

enum QueryDisposition {
    Return(ResolvedRecord),
    Wait(Arc<LookupFlight>),
}

impl DeterministicCache {
    fn lookup(&mut self, question: &DnsQuestion, now: Instant) -> Option<CacheHit> {
        let expired = self
            .entries
            .get(question)
            .is_some_and(|entry| now >= entry.stale_until);
        if expired {
            self.remove(question);
            return None;
        }
        self.entries.get(question).map(|entry| CacheHit {
            answer: entry.answer.clone(),
            age: now.saturating_duration_since(entry.inserted_at),
            stale: now >= entry.fresh_until,
            refresh_due: now >= entry.refresh_at,
        })
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "cache insertion keeps the immutable DNS policy values explicit and allocation-free"
    )]
    fn insert(
        &mut self,
        question: DnsQuestion,
        answer: Arc<CachedDnsResponse>,
        ttl: Duration,
        capacity: usize,
        stale_if_error: Duration,
        prefetch_max: Duration,
        now: Instant,
    ) -> usize {
        if capacity == 0 || ttl.is_zero() {
            return 0;
        }
        self.remove(&question);
        let mut evicted = 0;
        while self.entries.len() >= capacity {
            let Some((sequence, oldest)) = self.insertion_order.pop_first() else {
                evicted += self.entries.len();
                self.entries.clear();
                break;
            };
            if self
                .entries
                .get(&oldest)
                .is_some_and(|entry| entry.sequence == sequence)
            {
                self.entries.remove(&oldest);
                evicted += 1;
            }
        }
        let sequence = self.next_sequence;
        self.next_sequence = self.next_sequence.wrapping_add(1);
        if self.next_sequence == 0 {
            self.renumber();
        }
        let fresh_until = now.checked_add(ttl).unwrap_or(now);
        let refresh_ahead = (ttl / 10).min(prefetch_max);
        let refresh_at = fresh_until.checked_sub(refresh_ahead).unwrap_or(now);
        let stale_until = if answer.positive {
            fresh_until
                .checked_add(stale_if_error)
                .unwrap_or(fresh_until)
        } else {
            fresh_until
        };
        self.insertion_order.insert(sequence, question.clone());
        self.entries.insert(
            question,
            CacheEntry {
                answer,
                inserted_at: now,
                refresh_at,
                fresh_until,
                stale_until,
                sequence,
            },
        );
        evicted
    }

    fn remove(&mut self, question: &DnsQuestion) {
        if let Some(entry) = self.entries.remove(question) {
            self.insertion_order.remove(&entry.sequence);
        }
    }

    fn renumber(&mut self) {
        let domains = self.insertion_order.values().cloned().collect::<Vec<_>>();
        self.insertion_order.clear();
        for (sequence, domain) in domains.into_iter().enumerate() {
            let sequence = sequence as u64;
            if let Some(entry) = self.entries.get_mut(&domain) {
                entry.sequence = sequence;
                self.insertion_order.insert(sequence, domain);
            }
        }
        self.next_sequence = self.entries.len() as u64;
    }

    fn clear(&mut self) -> usize {
        let removed = self.entries.len();
        self.entries.clear();
        self.insertion_order.clear();
        removed
    }

    fn counts(&mut self, now: Instant) -> (usize, usize) {
        let expired = self
            .entries
            .iter()
            .filter_map(|(question, entry)| (now >= entry.stale_until).then_some(question.clone()))
            .collect::<Vec<_>>();
        for question in expired {
            self.remove(&question);
        }
        self.entries.values().fold((0, 0), |(fresh, stale), entry| {
            if now < entry.fresh_until {
                (fresh + 1, stale)
            } else {
                (fresh, stale + 1)
            }
        })
    }
}

struct LookupFlight {
    result: Mutex<Option<Result<ResolvedRecord, DnsRuntimeError>>>,
    ready: Notify,
}

impl LookupFlight {
    fn new() -> Self {
        Self {
            result: Mutex::new(None),
            ready: Notify::new(),
        }
    }

    fn finish(&self, result: Result<ResolvedRecord, DnsRuntimeError>) {
        *self.result.lock().expect("DNS lookup flight lock") = Some(result);
        self.ready.notify_waiters();
    }

    async fn wait(&self) -> Result<ResolvedRecord, DnsRuntimeError> {
        loop {
            let notified = self.ready.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if let Some(result) = self.result.lock().expect("DNS lookup flight lock").clone() {
                return result;
            }
            notified.await;
        }
    }
}

#[derive(Default)]
struct PlanTelemetry {
    queries: AtomicU64,
    fresh_cache_hits: AtomicU64,
    cache_misses: AtomicU64,
    coalesced_queries: AtomicU64,
    refreshes_started: AtomicU64,
    stale_answers: AtomicU64,
    cache_evictions: AtomicU64,
    cache_flushes: AtomicU64,
    host_answers: AtomicU64,
}

#[derive(Default)]
struct UpstreamTelemetry {
    attempts: AtomicU64,
    successes: AtomicU64,
    negative_answers: AtomicU64,
    failures: AtomicU64,
    timeouts: AtomicU64,
    rejected_answers: AtomicU64,
    canceled_attempts: AtomicU64,
    total_latency_micros: AtomicU64,
}

impl UpstreamTelemetry {
    fn snapshot(&self, descriptor: DnsUpstreamDescriptor) -> DnsUpstreamRuntimeSnapshot {
        DnsUpstreamRuntimeSnapshot {
            upstream: descriptor.upstream,
            transport: descriptor.transport,
            bootstrap: descriptor.bootstrap,
            egress: descriptor.egress,
            attempts: self.attempts.load(Ordering::Relaxed),
            successes: self.successes.load(Ordering::Relaxed),
            negative_answers: self.negative_answers.load(Ordering::Relaxed),
            failures: self.failures.load(Ordering::Relaxed),
            timeouts: self.timeouts.load(Ordering::Relaxed),
            rejected_answers: self.rejected_answers.load(Ordering::Relaxed),
            canceled_attempts: self.canceled_attempts.load(Ordering::Relaxed),
            total_latency_micros: self.total_latency_micros.load(Ordering::Relaxed),
        }
    }
}

fn record_resolution_has_addresses(
    result: &Result<ResolvedRecord, DnsRuntimeError>,
    record_type: RecordType,
) -> bool {
    result.as_ref().is_ok_and(|resolved| {
        resolved.answer.message.answers.iter().any(|record| {
            matches!(
                (record_type, &record.data),
                (RecordType::A, RData::A(_)) | (RecordType::AAAA, RData::AAAA(_))
            )
        })
    })
}

fn record_resolution_is_nx_domain(result: &Result<ResolvedRecord, DnsRuntimeError>) -> bool {
    result.as_ref().is_ok_and(|resolved| {
        resolved.answer.message.metadata.response_code == ResponseCode::NXDomain
    })
}

fn stale_fallback_allowed(error: &DnsRuntimeError) -> bool {
    matches!(
        error,
        DnsRuntimeError::Timeout { .. } | DnsRuntimeError::AllUpstreamsFailed { .. }
    )
}

fn merge_address_record_results(
    plan: &DnsPlanId,
    domain: &DomainName,
    maximum: usize,
    results: Vec<Result<ResolvedRecord, DnsRuntimeError>>,
) -> Result<Arc<[IpAddr]>, DnsRuntimeError> {
    let mut addresses = Vec::new();
    let mut seen = HashSet::with_capacity(maximum.saturating_add(1));
    let mut failure = None;
    let mut saw_nx_domain = false;
    for result in results {
        match result {
            Ok(resolved) => {
                saw_nx_domain |=
                    resolved.answer.message.metadata.response_code == ResponseCode::NXDomain;
                for record in &resolved.answer.message.answers {
                    let address = match &record.data {
                        RData::A(address) => Some(IpAddr::V4(address.0)),
                        RData::AAAA(address) => Some(IpAddr::V6(address.0)),
                        _ => None,
                    };
                    if let Some(address) = address
                        && seen.insert(address)
                    {
                        if seen.len() > maximum {
                            return Err(DnsRuntimeError::TooManyAnswers {
                                plan: plan.clone(),
                                domain: domain.clone(),
                                count: seen.len(),
                                maximum,
                            });
                        }
                        addresses.push(address);
                    }
                }
            }
            Err(error) => failure = Some(error),
        }
    }
    if !addresses.is_empty() {
        if saw_nx_domain {
            return Err(DnsRuntimeError::AllUpstreamsFailed {
                plan: plan.clone(),
                domain: domain.clone(),
                last_upstream: DnsUpstreamId::parse("inconsistent")
                    .expect("static DNS upstream ID"),
                message: "DNS server returned inconsistent NXDOMAIN and positive address answers"
                    .to_string(),
            });
        }
        return Ok(Arc::from(addresses));
    }
    if let Some(error) = failure {
        return Err(error);
    }
    Err(DnsRuntimeError::NoRecords {
        plan: plan.clone(),
        domain: domain.clone(),
    })
}

fn negative_cached_response(
    question: &DnsQuestion,
    response_code: ResponseCode,
) -> CachedDnsResponse {
    let mut message = Message::response(0, OpCode::Query);
    if let Ok(query) = question.as_query() {
        message.add_query(query);
    }
    message.metadata.response_code = response_code;
    CachedDnsResponse {
        message,
        positive: false,
    }
}

fn hosts_response(
    question: &DnsQuestion,
    addresses: &[IpAddr],
    ttl: Duration,
) -> CachedDnsResponse {
    let mut message = Message::response(0, OpCode::Query);
    message.metadata.authoritative = true;
    let Ok(query) = question.as_query() else {
        return CachedDnsResponse {
            message,
            positive: false,
        };
    };
    message.add_query(query.clone());
    let ttl = dns_wire_ttl(ttl);
    for address in addresses {
        let data = match (question.record_type, address) {
            (RecordType::A, IpAddr::V4(address)) => RData::A(A(*address)),
            (RecordType::AAAA, IpAddr::V6(address)) => RData::AAAA(AAAA(*address)),
            _ => continue,
        };
        message.add_answer(Record::from_rdata(query.name().clone(), ttl, data));
    }
    CachedDnsResponse {
        positive: !message.answers.is_empty(),
        message,
    }
}

fn validate_expected_addresses(
    question: &DnsQuestion,
    answer: &CachedDnsResponse,
    expected: &[IpNet],
) -> Result<(), String> {
    if expected.is_empty()
        || !answer.positive
        || !matches!(question.record_type, RecordType::A | RecordType::AAAA)
    {
        return Ok(());
    }
    let addresses = answer
        .message
        .answers
        .iter()
        .filter_map(|record| match (question.record_type, &record.data) {
            (RecordType::A, RData::A(address)) => Some(IpAddr::V4(address.0)),
            (RecordType::AAAA, RData::AAAA(address)) => Some(IpAddr::V6(address.0)),
            _ => None,
        })
        .collect::<Vec<_>>();
    if addresses.is_empty() {
        return Err("DNS answer did not contain an address required by expected_cidrs".to_string());
    }
    if let Some(address) = addresses
        .iter()
        .find(|address| !expected.iter().any(|cidr| cidr.contains(*address)))
    {
        return Err(format!(
            "DNS answer address {address} is outside the plan's expected_cidrs"
        ));
    }
    Ok(())
}

enum DnsResponseValidationError {
    TooManyAnswers(usize),
    Failed(String),
}

fn normalize_backend_response(
    question: &DnsQuestion,
    answer: DnsBackendResponse,
    limits: DnsPlanLimits,
) -> Result<(Arc<CachedDnsResponse>, Duration), DnsResponseValidationError> {
    let expected_query = question
        .as_query()
        .map_err(|error| DnsResponseValidationError::Failed(format!("{error:?}")))?;
    let mut message = answer.message;
    if message.metadata.message_type != MessageType::Response
        || message.metadata.op_code != OpCode::Query
        || message.metadata.truncation
        || message.queries.as_slice() != [expected_query]
    {
        return Err(DnsResponseValidationError::Failed(
            "DNS response does not match the normalized question".to_string(),
        ));
    }
    if !matches!(
        message.metadata.response_code,
        ResponseCode::NoError | ResponseCode::NXDomain
    ) {
        return Err(DnsResponseValidationError::Failed(format!(
            "DNS response code is {}",
            message.metadata.response_code
        )));
    }
    if message.answers.len() > limits.max_answers {
        return Err(DnsResponseValidationError::TooManyAnswers(
            message.answers.len(),
        ));
    }
    let record_count = message
        .answers
        .len()
        .saturating_add(message.authorities.len())
        .saturating_add(message.additionals.len());
    if record_count > MAX_DNS_CACHED_RECORDS {
        return Err(DnsResponseValidationError::Failed(format!(
            "DNS response has {record_count} records; maximum is {MAX_DNS_CACHED_RECORDS}"
        )));
    }
    if message.signature.is_some() {
        return Err(DnsResponseValidationError::Failed(
            "TSIG/SIG(0) responses cannot be rewritten by the local resolver".to_string(),
        ));
    }
    message.metadata.id = 0;
    message.edns = None;
    let positive =
        message.metadata.response_code == ResponseCode::NoError && !message.answers.is_empty();
    let message_ttl = if positive {
        minimum_record_ttl(&message)
    } else {
        negative_message_ttl(&message)
    };
    let source_ttl = match (answer.ttl, message_ttl) {
        (Some(backend), Some(message)) => Some(backend.min(message)),
        (backend, message) => backend.or(message),
    };
    let (fallback, cap) = if positive {
        (SYSTEM_POSITIVE_FALLBACK_TTL, limits.positive_ttl_cap)
    } else {
        (NEGATIVE_FALLBACK_TTL, limits.negative_ttl_cap)
    };
    let ttl = effective_ttl(source_ttl, fallback, cap);
    clamp_message_ttls(&mut message, cap);
    if !matches!(
        message.to_vec(),
        Ok(wire) if wire.len() <= MAX_DNS_WIRE_MESSAGE_BYTES
    ) {
        return Err(DnsResponseValidationError::Failed(
            "DNS response exceeds the wire-message limit".to_string(),
        ));
    }
    Ok((Arc::new(CachedDnsResponse { message, positive }), ttl))
}

fn minimum_record_ttl(message: &Message) -> Option<Duration> {
    message
        .answers
        .iter()
        .chain(message.authorities.iter())
        .chain(message.additionals.iter())
        .map(|record| Duration::from_secs(u64::from(record.ttl)))
        .min()
}

fn negative_message_ttl(message: &Message) -> Option<Duration> {
    message
        .authorities
        .iter()
        .find_map(|record| match &record.data {
            RData::SOA(soa) => Some(Duration::from_secs(u64::from(record.ttl.min(soa.minimum)))),
            _ => None,
        })
}

fn clamp_message_ttls(message: &mut Message, cap: Duration) {
    let cap = u32::try_from(cap.as_secs()).unwrap_or(u32::MAX);
    for record in message
        .answers
        .iter_mut()
        .chain(message.authorities.iter_mut())
        .chain(message.additionals.iter_mut())
    {
        record.ttl = record.ttl.min(cap);
    }
}

fn render_resolved_message(resolved: &ResolvedRecord, ttl_cap: Option<Duration>) -> Message {
    let mut message = resolved.answer.message.clone();
    let elapsed = u32::try_from(resolved.age.as_secs()).unwrap_or(u32::MAX);
    let stale_ttl = u32::try_from(DEFAULT_DNS_STALE_ANSWER_TTL.as_secs()).unwrap_or(u32::MAX);
    let caller_cap = ttl_cap.map(|ttl| u32::try_from(ttl.as_secs()).unwrap_or(u32::MAX));
    for record in message
        .answers
        .iter_mut()
        .chain(message.authorities.iter_mut())
        .chain(message.additionals.iter_mut())
    {
        let ttl = if resolved.stale {
            record.ttl.min(stale_ttl)
        } else {
            record.ttl.saturating_sub(elapsed)
        };
        record.ttl = caller_cap.map_or(ttl, |cap| ttl.min(cap));
    }
    message
}

struct ExplicitSystemDnsBackend;

impl DnsQueryBackend for ExplicitSystemDnsBackend {
    fn query(&self, question: DnsQuestion) -> DnsRecordBackendFuture {
        Box::pin(async move {
            if !matches!(question.record_type, RecordType::A | RecordType::AAAA) {
                return Err(DnsBackendError::Failed(
                    "the explicit operating-system resolver supports only address records"
                        .to_string(),
                ));
            }
            let addresses = tokio::net::lookup_host((question.domain.as_str(), 0))
                .await
                .map_err(|error| match error.kind() {
                    std::io::ErrorKind::TimedOut => DnsBackendError::Timeout,
                    std::io::ErrorKind::NotFound => DnsBackendError::NoRecords { ttl: None },
                    _ => DnsBackendError::Failed(bounded_backend_message(error.to_string())),
                })?;
            let query = question.as_query()?;
            let mut message = Message::response(0, OpCode::Query);
            message.add_query(query.clone());
            let mut saw_address = false;
            let mut seen = HashSet::new();
            for address in addresses.map(|address| address.ip()) {
                saw_address = true;
                let data = match (question.record_type, address) {
                    (RecordType::A, IpAddr::V4(address)) => RData::A(A(address)),
                    (RecordType::AAAA, IpAddr::V6(address)) => RData::AAAA(AAAA(address)),
                    _ => continue,
                };
                if !seen.insert(address) {
                    continue;
                }
                message.add_answer(Record::from_rdata(
                    query.name().clone(),
                    dns_wire_ttl(SYSTEM_POSITIVE_FALLBACK_TTL),
                    data,
                ));
            }
            if message.answers.is_empty() && !saw_address {
                return Err(DnsBackendError::NoRecords { ttl: None });
            }
            Ok(DnsBackendResponse::new(message, None))
        })
    }
}

pub(crate) struct RoutedTcpDnsBackend {
    inner: Arc<RoutedTcpDnsBackendInner>,
}

struct RoutedTcpDnsBackendInner {
    bootstrap: SocketAddr,
    server_name: Option<String>,
    connect_timeout: Duration,
    tls: Option<Arc<rustls::ClientConfig>>,
    connector: Arc<dyn DnsTcpConnector>,
}

impl RoutedTcpDnsBackend {
    pub(crate) fn compile_with_connector(
        plan: &CompiledDnsPlan,
        upstream: &CompiledDnsUpstream,
        connector: Arc<dyn DnsTcpConnector>,
    ) -> Result<Self, DnsRuntimeError> {
        let (bootstrap, server_name) = match upstream.endpoint() {
            DnsUpstreamEndpoint::Tcp { bootstrap } => (*bootstrap, None),
            DnsUpstreamEndpoint::Tls {
                bootstrap,
                server_name,
            } => (*bootstrap, Some(server_name.to_string())),
            _ => {
                return Err(DnsRuntimeError::PolicyInvariant(format!(
                    "non-TCP/DoT upstream {} reached the routed TCP DNS backend",
                    upstream.id()
                )));
            }
        };
        if let Some(server_name) = server_name.as_deref() {
            ServerName::try_from(server_name).map_err(|error| DnsRuntimeError::Build {
                upstream: upstream.id().clone(),
                message: bounded_backend_message(format!("invalid DoT TLS identity: {error}")),
            })?;
        }
        let tls = server_name
            .as_ref()
            .map(|_| Arc::new(dns_tls_client_config(Vec::new())));
        Ok(Self {
            inner: Arc::new(RoutedTcpDnsBackendInner {
                bootstrap,
                server_name,
                connect_timeout: plan.limits().lookup_timeout,
                tls,
                connector,
            }),
        })
    }
}

impl RoutedTcpDnsBackendInner {
    async fn connect(&self) -> Result<DnsTcpStream, DnsBackendError> {
        let stream = self
            .connector
            .connect(self.bootstrap, self.connect_timeout)
            .await?;
        let Some(server_name) = self.server_name.as_ref() else {
            return Ok(stream);
        };
        let server_name = ServerName::try_from(server_name.clone())
            .map_err(|error| DnsBackendError::Failed(error.to_string()))?;
        let tls = TlsConnector::from(
            self.tls
                .as_ref()
                .expect("DoT TLS configuration was compiled")
                .clone(),
        )
        .connect(server_name, stream)
        .await
        .map_err(|error| DnsBackendError::Failed(bounded_backend_message(error.to_string())))?;
        Ok(Box::new(tls))
    }

    async fn query(
        self: Arc<Self>,
        question: DnsQuestion,
    ) -> Result<DnsBackendResponse, DnsBackendError> {
        let query = question.as_query()?;
        let mut request = Message::new(0, MessageType::Query, OpCode::Query);
        request.metadata.recursion_desired = true;
        request.add_query(query.clone());
        let wire = request
            .to_vec()
            .map_err(|error| DnsBackendError::Failed(error.to_string()))?;
        let wire_len = u16::try_from(wire.len()).map_err(|_| {
            DnsBackendError::Failed("DNS TCP request exceeds the wire-message limit".to_string())
        })?;
        let mut stream = self.connect().await?;
        stream
            .write_all(&wire_len.to_be_bytes())
            .await
            .map_err(map_io_backend_error)?;
        stream
            .write_all(&wire)
            .await
            .map_err(map_io_backend_error)?;
        stream.flush().await.map_err(map_io_backend_error)?;

        let mut response_len = [0_u8; 2];
        stream
            .read_exact(&mut response_len)
            .await
            .map_err(map_io_backend_error)?;
        let response_len = usize::from(u16::from_be_bytes(response_len));
        if response_len == 0 {
            return Err(DnsBackendError::Failed(
                "DNS TCP response is empty".to_string(),
            ));
        }
        let mut wire_response = vec![0_u8; response_len];
        stream
            .read_exact(&mut wire_response)
            .await
            .map_err(map_io_backend_error)?;
        let response = DnsResponse::from_buffer(wire_response).map_err(|error| {
            DnsBackendError::Failed(format!("invalid DNS TCP message: {error}"))
        })?;
        if response.metadata.id != 0
            || response.metadata.op_code != OpCode::Query
            || response.metadata.truncation
            || response.queries.len() != 1
            || response.queries[0] != query
        {
            return Err(DnsBackendError::Failed(
                "DNS TCP response does not match the query".to_string(),
            ));
        }
        let ttl = if response.metadata.response_code == ResponseCode::NoError
            && !response.answers.is_empty()
        {
            response
                .answers
                .iter()
                .map(|record| Duration::from_secs(u64::from(record.ttl)))
                .min()
        } else {
            response
                .negative_ttl()
                .map(|ttl| Duration::from_secs(u64::from(ttl)))
        };
        Ok(DnsBackendResponse::new(response.into_message(), ttl))
    }
}

impl DnsQueryBackend for RoutedTcpDnsBackend {
    fn query(&self, question: DnsQuestion) -> DnsRecordBackendFuture {
        let inner = self.inner.clone();
        Box::pin(async move { inner.query(question).await })
    }
}

pub(crate) struct DohDnsBackend {
    inner: Arc<DohDnsBackendInner>,
}

struct DohDnsBackendInner {
    bootstrap: SocketAddr,
    server_name: String,
    authority: String,
    path: String,
    connect_timeout: Duration,
    tls: Arc<rustls::ClientConfig>,
    connector: Arc<dyn DnsTcpConnector>,
    sender: tokio::sync::Mutex<Option<h2::client::SendRequest<Bytes>>>,
}

impl DohDnsBackend {
    fn compile(
        plan: &CompiledDnsPlan,
        upstream: &CompiledDnsUpstream,
        socket_policy: DnsNativeSocketPolicy,
    ) -> Result<Self, DnsRuntimeError> {
        if let Some(bootstrap) = upstream.endpoint().bootstrap() {
            ensure_source_family(socket_policy.source_ip, bootstrap.ip()).map_err(|error| {
                DnsRuntimeError::Build {
                    upstream: upstream.id().clone(),
                    message: error.to_string(),
                }
            })?;
        }
        Self::compile_with_connector(
            plan,
            upstream,
            Arc::new(DirectDnsTcpConnector {
                policy: socket_policy,
            }),
        )
    }

    pub(crate) fn compile_with_connector(
        plan: &CompiledDnsPlan,
        upstream: &CompiledDnsUpstream,
        connector: Arc<dyn DnsTcpConnector>,
    ) -> Result<Self, DnsRuntimeError> {
        let DnsUpstreamEndpoint::Https {
            bootstrap,
            server_name,
            path,
        } = upstream.endpoint()
        else {
            return Err(DnsRuntimeError::PolicyInvariant(format!(
                "non-DoH upstream {} reached the DoH backend",
                upstream.id()
            )));
        };
        let authority = if bootstrap.port() == 443 {
            server_name.to_string()
        } else {
            format!("{server_name}:{}", bootstrap.port())
        };
        let uri = format!("https://{authority}{path}")
            .parse::<http::Uri>()
            .map_err(|error| DnsRuntimeError::Build {
                upstream: upstream.id().clone(),
                message: bounded_backend_message(format!("invalid DoH URI: {error}")),
            })?;
        if uri.scheme_str() != Some("https")
            || uri
                .authority()
                .is_none_or(|value| value.as_str() != authority)
            || uri.path() != path
            || uri.query().is_some()
        {
            return Err(DnsRuntimeError::Build {
                upstream: upstream.id().clone(),
                message: "DoH identity, authority, bootstrap port, or path changed during parsing"
                    .to_string(),
            });
        }
        ServerName::try_from(server_name.as_str()).map_err(|error| DnsRuntimeError::Build {
            upstream: upstream.id().clone(),
            message: bounded_backend_message(format!("invalid DoH TLS identity: {error}")),
        })?;
        let tls = dns_tls_client_config(vec![b"h2".to_vec()]);
        Ok(Self {
            inner: Arc::new(DohDnsBackendInner {
                bootstrap: *bootstrap,
                server_name: server_name.to_string(),
                authority,
                path: path.clone(),
                connect_timeout: plan.limits().lookup_timeout,
                tls: Arc::new(tls),
                connector,
                sender: tokio::sync::Mutex::new(None),
            }),
        })
    }
}

impl DohDnsBackendInner {
    async fn sender(&self) -> Result<h2::client::SendRequest<Bytes>, DnsBackendError> {
        let mut slot = self.sender.lock().await;
        if let Some(sender) = slot.as_ref() {
            return Ok(sender.clone());
        }
        let stream = self
            .connector
            .connect(self.bootstrap, self.connect_timeout)
            .await?;
        let server_name = ServerName::try_from(self.server_name.clone())
            .map_err(|error| DnsBackendError::Failed(error.to_string()))?;
        let tls = tokio::time::timeout(
            self.connect_timeout,
            TlsConnector::from(self.tls.clone()).connect(server_name, stream),
        )
        .await
        .map_err(|_| DnsBackendError::Timeout)?
        .map_err(|error| DnsBackendError::Failed(bounded_backend_message(error.to_string())))?;
        let (sender, connection) = tokio::time::timeout(
            self.connect_timeout,
            h2::client::Builder::new().handshake(tls),
        )
        .await
        .map_err(|_| DnsBackendError::Timeout)?
        .map_err(|error| DnsBackendError::Failed(bounded_backend_message(error.to_string())))?;
        tokio::spawn(async move {
            let _ = connection.await;
        });
        *slot = Some(sender.clone());
        Ok(sender)
    }

    async fn invalidate_sender(&self) {
        *self.sender.lock().await = None;
    }

    async fn query(
        self: Arc<Self>,
        question: DnsQuestion,
    ) -> Result<DnsBackendResponse, DnsBackendError> {
        let query = question.as_query()?;
        let mut request = Message::new(0, MessageType::Query, OpCode::Query);
        request.metadata.recursion_desired = true;
        request.add_query(query.clone());
        let wire = request
            .to_vec()
            .map_err(|error| DnsBackendError::Failed(error.to_string()))?;
        let uri = format!("https://{}{}", self.authority, self.path)
            .parse::<http::Uri>()
            .map_err(|error| DnsBackendError::Failed(error.to_string()))?;
        let request = Request::builder()
            .method(Method::POST)
            .version(Version::HTTP_2)
            .uri(uri)
            .header(header::ACCEPT, DNS_MESSAGE_CONTENT_TYPE)
            .header(header::CONTENT_TYPE, DNS_MESSAGE_CONTENT_TYPE)
            .header(header::CONTENT_LENGTH, wire.len())
            .body(())
            .map_err(|error| DnsBackendError::Failed(error.to_string()))?;
        let mut sender = self.sender().await?;
        sender = match sender.ready().await {
            Ok(sender) => sender,
            Err(error) => {
                self.invalidate_sender().await;
                return Err(DnsBackendError::Failed(bounded_backend_message(
                    error.to_string(),
                )));
            }
        };
        let (response, mut body) = sender
            .send_request(request, false)
            .map_err(|error| DnsBackendError::Failed(bounded_backend_message(error.to_string())))?;
        body.send_data(Bytes::from(wire), true)
            .map_err(|error| DnsBackendError::Failed(bounded_backend_message(error.to_string())))?;
        let response = match response.await {
            Ok(response) => response,
            Err(error) => {
                self.invalidate_sender().await;
                return Err(DnsBackendError::Failed(bounded_backend_message(
                    error.to_string(),
                )));
            }
        };
        if !response.status().is_success() {
            return Err(DnsBackendError::Failed(format!(
                "DoH server returned HTTP {}",
                response.status()
            )));
        }
        let content_type = response
            .headers()
            .get(header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.split(';').next())
            .map(str::trim);
        if content_type != Some(DNS_MESSAGE_CONTENT_TYPE) {
            return Err(DnsBackendError::Failed(
                "DoH response Content-Type is not application/dns-message".to_string(),
            ));
        }
        let declared_length = response
            .headers()
            .get(header::CONTENT_LENGTH)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.parse::<usize>().ok());
        if declared_length.is_some_and(|length| length > MAX_DOH_RESPONSE_BYTES) {
            return Err(DnsBackendError::Failed(
                "DoH response exceeds the DNS wire-message limit".to_string(),
            ));
        }
        let mut wire_response = Vec::with_capacity(declared_length.unwrap_or(512).min(4_096));
        let mut stream = response.into_body();
        while let Some(chunk) = stream.data().await {
            let chunk = chunk.map_err(|error| {
                DnsBackendError::Failed(bounded_backend_message(error.to_string()))
            })?;
            let chunk_len = chunk.len();
            let new_len = wire_response.len().checked_add(chunk_len).ok_or_else(|| {
                DnsBackendError::Failed("DoH response length overflow".to_string())
            })?;
            if new_len > MAX_DOH_RESPONSE_BYTES {
                return Err(DnsBackendError::Failed(
                    "DoH response exceeds the DNS wire-message limit".to_string(),
                ));
            }
            wire_response.extend_from_slice(&chunk);
            stream
                .flow_control()
                .release_capacity(chunk_len)
                .map_err(|error| {
                    DnsBackendError::Failed(bounded_backend_message(error.to_string()))
                })?;
        }
        if declared_length.is_some_and(|length| length != wire_response.len()) {
            return Err(DnsBackendError::Failed(
                "DoH response Content-Length does not match its body".to_string(),
            ));
        }
        let response = DnsResponse::from_buffer(wire_response).map_err(|error| {
            DnsBackendError::Failed(format!("invalid DoH DNS message: {error}"))
        })?;
        if response.metadata.id != 0
            || response.metadata.op_code != OpCode::Query
            || response.metadata.truncation
            || response.queries.len() != 1
            || response.queries[0] != query
        {
            return Err(DnsBackendError::Failed(
                "DoH response does not match the DNS query".to_string(),
            ));
        }
        let ttl = if response.metadata.response_code == ResponseCode::NoError
            && !response.answers.is_empty()
        {
            response
                .answers
                .iter()
                .map(|record| Duration::from_secs(u64::from(record.ttl)))
                .min()
        } else {
            response
                .negative_ttl()
                .map(|ttl| Duration::from_secs(u64::from(ttl)))
        };
        Ok(DnsBackendResponse::new(response.into_message(), ttl))
    }
}

impl DnsQueryBackend for DohDnsBackend {
    fn query(&self, question: DnsQuestion) -> DnsRecordBackendFuture {
        let inner = self.inner.clone();
        Box::pin(async move { inner.query(question).await })
    }
}

fn map_io_backend_error(error: io::Error) -> DnsBackendError {
    if error.kind() == io::ErrorKind::TimedOut {
        DnsBackendError::Timeout
    } else {
        DnsBackendError::Failed(bounded_backend_message(error.to_string()))
    }
}

fn dns_tls_client_config(alpn_protocols: Vec<Vec<u8>>) -> rustls::ClientConfig {
    let mut roots = rustls::RootCertStore::empty();
    roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
    let mut config = rustls::ClientConfig::builder_with_protocol_versions(&[
        &rustls::version::TLS13,
        &rustls::version::TLS12,
    ])
    .with_root_certificates(roots)
    .with_no_client_auth();
    config.alpn_protocols = alpn_protocols;
    config
}

#[cfg(test)]
struct StaticTestBackendFactory {
    upstream: DnsUpstreamId,
    answers: Arc<HashMap<DomainName, Vec<IpAddr>>>,
}

#[cfg(test)]
impl DnsBackendFactory for StaticTestBackendFactory {
    fn build_backend(
        &self,
        _plan: &CompiledDnsPlan,
        upstream: &CompiledDnsUpstream,
    ) -> Result<Arc<dyn DnsQueryBackend>, DnsRuntimeError> {
        if upstream.id() != &self.upstream {
            return Err(DnsRuntimeError::PolicyInvariant(
                "static test DNS factory received an unknown upstream".to_string(),
            ));
        }
        Ok(Arc::new(StaticTestBackend {
            answers: self.answers.clone(),
        }))
    }
}

#[cfg(test)]
struct StaticTestBackend {
    answers: Arc<HashMap<DomainName, Vec<IpAddr>>>,
}

#[cfg(test)]
impl DnsQueryBackend for StaticTestBackend {
    fn query(&self, question: DnsQuestion) -> DnsRecordBackendFuture {
        let answer = self.answers.get(&question.domain).cloned();
        Box::pin(async move {
            match answer {
                Some(addresses) => {
                    let query = question.as_query()?;
                    let mut message = Message::response(0, OpCode::Query);
                    message.add_query(query.clone());
                    for address in addresses {
                        let data = match (question.record_type, address) {
                            (RecordType::A, IpAddr::V4(address)) => RData::A(A(address)),
                            (RecordType::AAAA, IpAddr::V6(address)) => RData::AAAA(AAAA(address)),
                            _ => continue,
                        };
                        message.add_answer(Record::from_rdata(query.name().clone(), 60, data));
                    }
                    Ok(DnsBackendResponse::new(
                        message,
                        Some(Duration::from_secs(60)),
                    ))
                }
                None => Err(DnsBackendError::NoRecords { ttl: None }),
            }
        })
    }
}

#[derive(Clone)]
struct ConfiguredDnsRuntimeProvider {
    handle: TokioHandle,
    socket_policy: DnsNativeSocketPolicy,
}

impl ConfiguredDnsRuntimeProvider {
    fn new(socket_policy: DnsNativeSocketPolicy) -> Self {
        Self {
            handle: TokioHandle::default(),
            socket_policy,
        }
    }
}

impl RuntimeProvider for ConfiguredDnsRuntimeProvider {
    type Handle = TokioHandle;
    type Timer = TokioTime;
    type Udp = UdpSocket;
    type Tcp = AsyncIoTokioAsStd<TcpStream>;

    fn create_handle(&self) -> Self::Handle {
        self.handle.clone()
    }

    fn connect_tcp(
        &self,
        server_addr: SocketAddr,
        bind_addr: Option<SocketAddr>,
        wait_for: Option<Duration>,
    ) -> Pin<Box<dyn Send + Future<Output = Result<Self::Tcp, io::Error>>>> {
        let socket_policy = self.socket_policy.clone();
        Box::pin(async move {
            if bind_addr.is_some() && socket_policy.source_ip.is_some() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "DNS runtime and upstream both requested a source bind",
                ));
            }
            let socket = configured_tcp_socket(server_addr, &socket_policy)?;
            if let Some(bind_addr) = bind_addr {
                ensure_source_family(Some(bind_addr.ip()), server_addr.ip())?;
                socket.bind(bind_addr)?;
            }
            let wait_for = wait_for.unwrap_or(Duration::from_secs(5));
            let stream = tokio::time::timeout(wait_for, socket.connect(server_addr))
                .await
                .map_err(|_| {
                    io::Error::new(io::ErrorKind::TimedOut, "DNS TCP connect timed out")
                })??;
            Ok(AsyncIoTokioAsStd(stream))
        })
    }

    fn bind_udp(
        &self,
        local_addr: SocketAddr,
        server_addr: SocketAddr,
    ) -> Pin<Box<dyn Send + Future<Output = Result<Self::Udp, io::Error>>>> {
        let socket_policy = self.socket_policy.clone();
        Box::pin(async move {
            let bind_addr = match socket_policy.source_ip {
                Some(source_ip) => {
                    ensure_source_family(Some(source_ip), server_addr.ip())?;
                    SocketAddr::new(source_ip, 0)
                }
                None => local_addr,
            };
            let socket = StdUdpSocket::bind(bind_addr)?;
            socket_policy.native_sockets.configure_udp(
                &socket,
                NativeSocketRequest {
                    remote_addr: server_addr,
                    purpose: NativeEgressPurpose::Dns,
                },
            )?;
            socket.set_nonblocking(true)?;
            UdpSocket::from_std(socket)
        })
    }

    fn quic_binder(&self) -> Option<&dyn QuicSocketBinder> {
        Some(self)
    }
}

impl QuicSocketBinder for ConfiguredDnsRuntimeProvider {
    fn bind_quic(
        &self,
        local_addr: SocketAddr,
        server_addr: SocketAddr,
    ) -> Result<Arc<dyn quinn::AsyncUdpSocket>, io::Error> {
        let bind_addr = match self.socket_policy.source_ip {
            Some(source_ip) => {
                ensure_source_family(Some(source_ip), server_addr.ip())?;
                SocketAddr::new(source_ip, 0)
            }
            None => local_addr,
        };
        let socket = StdUdpSocket::bind(bind_addr)?;
        self.socket_policy.native_sockets.configure_udp(
            &socket,
            NativeSocketRequest {
                remote_addr: server_addr,
                purpose: NativeEgressPurpose::Dns,
            },
        )?;
        socket.set_nonblocking(true)?;
        quinn::Runtime::wrap_udp_socket(&quinn::TokioRuntime, socket)
    }
}

fn configured_tcp_socket(
    remote: SocketAddr,
    policy: &DnsNativeSocketPolicy,
) -> io::Result<TcpSocket> {
    ensure_source_family(policy.source_ip, remote.ip())?;
    let socket = match remote {
        SocketAddr::V4(_) => TcpSocket::new_v4()?,
        SocketAddr::V6(_) => TcpSocket::new_v6()?,
    };
    if let Some(source_ip) = policy.source_ip {
        socket.bind(SocketAddr::new(source_ip, 0))?;
    }
    socket.set_nodelay(true)?;
    policy.native_sockets.configure_tcp(
        &socket,
        NativeSocketRequest {
            remote_addr: remote,
            purpose: NativeEgressPurpose::Dns,
        },
    )?;
    Ok(socket)
}

fn ensure_source_family(source: Option<IpAddr>, remote: IpAddr) -> io::Result<()> {
    if source.is_some_and(|source| source.is_ipv4() != remote.is_ipv4()) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "DNS source and bootstrap IP families differ",
        ));
    }
    Ok(())
}

struct HickoryDnsBackend {
    resolver: Resolver<ConfiguredDnsRuntimeProvider>,
}

impl HickoryDnsBackend {
    fn compile(
        plan: &CompiledDnsPlan,
        upstream: &CompiledDnsUpstream,
        socket_policy: DnsNativeSocketPolicy,
    ) -> Result<Self, DnsRuntimeError> {
        let name_server = name_server_config(upstream.endpoint()).ok_or_else(|| {
            DnsRuntimeError::PolicyInvariant(format!(
                "upstream {} reached the wrong direct backend",
                upstream.id()
            ))
        })?;
        let bootstrap_ip = name_server.ip;
        let resolver_config = ResolverConfig::from_parts(None, Vec::new(), vec![name_server]);
        let limits = plan.limits();
        let mut options = ResolverOpts::default();
        options.ip_strategy = hickory_ip_strategy(plan.ip_strategy());
        options.timeout = limits.lookup_timeout;
        options.attempts = 1;
        options.cache_size = 0;
        options.use_hosts_file = ResolveHosts::Never;
        options.positive_max_ttl = Some(limits.positive_ttl_cap);
        options.negative_max_ttl = Some(limits.negative_ttl_cap);
        options.max_active_requests = limits.max_inflight;
        options.server_ordering_strategy = ServerOrderingStrategy::UserProvidedOrder;
        options.try_tcp_on_error =
            matches!(upstream.endpoint(), DnsUpstreamEndpoint::UdpTcp { .. });
        ensure_source_family(socket_policy.source_ip, bootstrap_ip).map_err(|error| {
            DnsRuntimeError::Build {
                upstream: upstream.id().clone(),
                message: error.to_string(),
            }
        })?;
        let resolver = Resolver::builder_with_config(
            resolver_config,
            ConfiguredDnsRuntimeProvider::new(socket_policy),
        )
        .with_options(options)
        .build()
        .map_err(|error| DnsRuntimeError::Build {
            upstream: upstream.id().clone(),
            message: bounded_backend_message(error.to_string()),
        })?;
        Ok(Self { resolver })
    }
}

impl DnsQueryBackend for HickoryDnsBackend {
    fn query(&self, question: DnsQuestion) -> DnsRecordBackendFuture {
        let resolver = self.resolver.clone();
        Box::pin(async move {
            // Hickory follows RFC 6761 by synthesizing loopback answers for
            // `.localhost` before consulting any configured name server. An
            // explicit MPTunnel upstream must never produce a hosts/system/
            // library-local answer, so fail closed at that boundary.
            if question.domain.as_str() == "localhost"
                || question.domain.as_str().ends_with(".localhost")
            {
                return Err(DnsBackendError::NoRecords { ttl: None });
            }
            let fqdn = format!("{}.", question.domain);
            match resolver.lookup(fqdn, question.record_type).await {
                Ok(lookup) => Ok(DnsBackendResponse::new(
                    lookup.message().clone(),
                    Some(
                        lookup
                            .valid_until()
                            .saturating_duration_since(Instant::now()),
                    ),
                )),
                Err(NetError::Timeout) => Err(DnsBackendError::Timeout),
                Err(NetError::Dns(HickoryDnsError::NoRecordsFound(no_records))) => {
                    let mut message = Message::response(0, OpCode::Query);
                    message.add_query(*no_records.query);
                    message.metadata.response_code = no_records.response_code;
                    if let Some(authorities) = no_records.authorities {
                        message.add_authorities(authorities.iter().cloned());
                    } else if let Some(soa) = no_records.soa {
                        message.add_authority((*soa).into_record_of_rdata());
                    }
                    Ok(DnsBackendResponse::new(
                        message,
                        no_records
                            .negative_ttl
                            .map(|ttl| Duration::from_secs(u64::from(ttl))),
                    ))
                }
                Err(error) => Err(DnsBackendError::Failed(bounded_backend_message(
                    error.to_string(),
                ))),
            }
        })
    }
}

fn name_server_config(endpoint: &DnsUpstreamEndpoint) -> Option<NameServerConfig> {
    let bootstrap = endpoint.bootstrap()?;
    let port = bootstrap.port();
    let connections = match endpoint {
        DnsUpstreamEndpoint::System | DnsUpstreamEndpoint::Https { .. } => return None,
        DnsUpstreamEndpoint::Udp { .. } => vec![connection(ProtocolConfig::Udp, port)],
        DnsUpstreamEndpoint::Tcp { .. } => vec![connection(ProtocolConfig::Tcp, port)],
        DnsUpstreamEndpoint::UdpTcp { .. } => vec![
            connection(ProtocolConfig::Udp, port),
            connection(ProtocolConfig::Tcp, port),
        ],
        DnsUpstreamEndpoint::Tls { server_name, .. } => vec![connection(
            ProtocolConfig::Tls {
                server_name: Arc::from(server_name.as_str()),
            },
            port,
        )],
        DnsUpstreamEndpoint::Quic { server_name, .. } => vec![connection(
            ProtocolConfig::Quic {
                server_name: Arc::from(server_name.as_str()),
            },
            port,
        )],
    };
    Some(NameServerConfig::new(bootstrap.ip(), true, connections))
}

fn connection(protocol: ProtocolConfig, port: u16) -> ConnectionConfig {
    let mut connection = ConnectionConfig::new(protocol);
    connection.port = port;
    connection
}

const fn hickory_ip_strategy(strategy: DnsIpStrategy) -> LookupIpStrategy {
    match strategy {
        DnsIpStrategy::Ipv4Only => LookupIpStrategy::Ipv4Only,
        DnsIpStrategy::Ipv6Only => LookupIpStrategy::Ipv6Only,
        DnsIpStrategy::Ipv4ThenIpv6 => LookupIpStrategy::Ipv4thenIpv6,
        DnsIpStrategy::Ipv6ThenIpv4 => LookupIpStrategy::Ipv6thenIpv4,
        DnsIpStrategy::Ipv4AndIpv6 => LookupIpStrategy::Ipv4AndIpv6,
        DnsIpStrategy::Ipv6AndIpv4 => LookupIpStrategy::Ipv6AndIpv4,
    }
}

fn effective_ttl(source: Option<Duration>, fallback: Duration, cap: Duration) -> Duration {
    source.unwrap_or(fallback).min(cap)
}

fn bounded_backend_message(message: String) -> String {
    if message.len() <= MAX_BACKEND_ERROR_BYTES {
        return message;
    }
    let mut boundary = MAX_BACKEND_ERROR_BYTES;
    while !message.is_char_boundary(boundary) {
        boundary -= 1;
    }
    message[..boundary].to_string()
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DnsRuntimeError {
    MissingEgressConnector {
        upstream: DnsUpstreamId,
        outbound: OutboundId,
    },
    RecursiveEgressConnector {
        upstream: DnsUpstreamId,
        outbound: OutboundId,
    },
    PrepublicationDnsRequiresDirect {
        plan: DnsPlanId,
        upstream: DnsUpstreamId,
        outbound: OutboundId,
    },
    PrepublicationSystemDns {
        plan: DnsPlanId,
        upstream: DnsUpstreamId,
    },
    UnsupportedEgressTransport {
        upstream: DnsUpstreamId,
        outbound: OutboundId,
    },
    Build {
        upstream: DnsUpstreamId,
        message: String,
    },
    PolicyInvariant(String),
    UnknownPlan(DnsPlanId),
    InvalidDomain {
        domain: String,
        message: String,
    },
    InvalidPort,
    AtCapacity {
        plan: DnsPlanId,
        limit: usize,
    },
    ProductAtCapacity {
        rejection: ProductAdmissionRejection,
    },
    Timeout {
        plan: DnsPlanId,
        domain: DomainName,
    },
    NoRecords {
        plan: DnsPlanId,
        domain: DomainName,
    },
    TooManyAnswers {
        plan: DnsPlanId,
        domain: DomainName,
        count: usize,
        maximum: usize,
    },
    AllUpstreamsFailed {
        plan: DnsPlanId,
        domain: DomainName,
        last_upstream: DnsUpstreamId,
        message: String,
    },
}

impl std::fmt::Display for DnsRuntimeError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::MissingEgressConnector { upstream, outbound } => write!(
                formatter,
                "DNS upstream {upstream} requires outbound {outbound}, but no matching DNS egress connector was injected"
            ),
            Self::RecursiveEgressConnector { upstream, outbound } => write!(
                formatter,
                "DNS upstream {upstream} cannot use outbound {outbound}: its control endpoint depends on DNS"
            ),
            Self::PrepublicationDnsRequiresDirect {
                plan,
                upstream,
                outbound,
            } => write!(
                formatter,
                "pre-publication DNS plan {plan} cannot use upstream {upstream} through outbound {outbound}"
            ),
            Self::PrepublicationSystemDns { plan, upstream } => write!(
                formatter,
                "pre-publication DNS plan {plan} cannot use system upstream {upstream}"
            ),
            Self::UnsupportedEgressTransport { upstream, outbound } => write!(
                formatter,
                "DNS upstream {upstream} cannot use outbound {outbound}: routed DNS currently requires TCP, DoT, or DoH"
            ),
            Self::Build { upstream, message } => {
                write!(
                    formatter,
                    "failed to build DNS upstream {upstream}: {message}"
                )
            }
            Self::PolicyInvariant(message) => {
                write!(formatter, "compiled DNS policy invariant failed: {message}")
            }
            Self::UnknownPlan(plan) => {
                write!(formatter, "DNS plan {plan} is not in this generation")
            }
            Self::InvalidDomain { domain, message } => {
                write!(formatter, "invalid DNS domain {domain:?}: {message}")
            }
            Self::InvalidPort => formatter.write_str("DNS result port must be non-zero"),
            Self::AtCapacity { plan, limit } => {
                write!(
                    formatter,
                    "DNS plan {plan} reached its {limit}-query in-flight limit"
                )
            }
            Self::ProductAtCapacity { rejection } => {
                write!(formatter, "Product DNS admission rejected: {rejection}")
            }
            Self::Timeout { plan, domain } => {
                write!(formatter, "DNS plan {plan} timed out resolving {domain}")
            }
            Self::NoRecords { plan, domain } => {
                write!(
                    formatter,
                    "DNS plan {plan} found no address records for {domain}"
                )
            }
            Self::TooManyAnswers {
                plan,
                domain,
                count,
                maximum,
            } => write!(
                formatter,
                "DNS plan {plan} returned {count} addresses for {domain}; maximum is {maximum}"
            ),
            Self::AllUpstreamsFailed {
                plan,
                domain,
                last_upstream,
                message,
            } => write!(
                formatter,
                "all DNS upstreams in plan {plan} failed for {domain}; last failure from {last_upstream}: {message}"
            ),
        }
    }
}

impl std::error::Error for DnsRuntimeError {}

#[cfg(test)]
#[path = "dns_test.rs"]
mod tests;
