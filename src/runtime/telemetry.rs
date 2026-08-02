//! Product-boundary traffic and logical-flow accounting.
//!
//! These counters observe each product byte or datagram once, before carrier
//! scheduling or after ordered delivery. Keeping this boundary independent of
//! carrier writes prevents multipath replication and reinjection from inflating
//! user-visible traffic.

use crate::product::{BalancerId, FlowContext, InboundId, Network, OutboundId, TargetHost};
use crate::protocol::{DatagramFlowId, SessionId, StreamId, TargetAddr};
use std::collections::HashMap;
use std::io;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::task::{Context, Poll};
use std::time::{Duration, Instant};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

/// Per-flow observability is bounded independently from forwarding capacity.
/// Aggregate counters remain exact when a deployment has more active flows.
pub(crate) const MAX_ACTIVE_FLOW_DETAIL_RECORDS: usize = 1_024;

pub(crate) fn active_flow_detail_capacity(max_streams: usize) -> usize {
    max_streams.min(MAX_ACTIVE_FLOW_DETAIL_RECORDS)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ProductFlowId {
    Reliable(StreamId),
    Datagram(DatagramFlowId),
    NativeReliable,
    NativeDatagram,
}

impl ProductFlowId {
    fn kind(self) -> ProductFlowKind {
        match self {
            Self::Reliable(_) | Self::NativeReliable => ProductFlowKind::Reliable,
            Self::Datagram(_) | Self::NativeDatagram => ProductFlowKind::Datagram,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ProductFlowOriginKind {
    LocalInbound,
    MppInbound,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ProductFlowOrigin {
    pub kind: ProductFlowOriginKind,
    pub inbound: InboundId,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ProductFlowSelection {
    /// The concrete leaf pinned for this flow.
    pub outbound: OutboundId,
    /// The configured balancer, when selection passed through one.
    pub balancer: Option<BalancerId>,
    /// The concrete balancer member. This equals `outbound` for balanced flows.
    pub member: Option<OutboundId>,
}

/// Immutable Product identity attached after routing and before payload I/O.
///
/// This scope deliberately contains no connector endpoint, credential, or
/// carrier detail. A scoped telemetry handle is cloned once per flow; payload
/// observation then touches only relaxed atomics.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ProductFlowScope {
    pub origin: ProductFlowOrigin,
    pub network: Network,
    pub target: TargetAddr,
    pub selection: ProductFlowSelection,
}

impl ProductFlowScope {
    pub(crate) fn from_flow(
        origin_kind: ProductFlowOriginKind,
        flow: &FlowContext,
        outbound: OutboundId,
        balancer: Option<BalancerId>,
    ) -> Self {
        let target = match flow.target().host() {
            TargetHost::Domain(domain) => TargetAddr::Domain {
                host: domain.as_str().to_string(),
                port: flow.target().port().get(),
            },
            TargetHost::Ip(address) => TargetAddr::Ip(std::net::SocketAddr::new(
                *address,
                flow.target().port().get(),
            )),
        };
        Self {
            origin: ProductFlowOrigin {
                kind: origin_kind,
                inbound: flow.inbound().clone(),
            },
            network: flow.network(),
            target,
            selection: ProductFlowSelection {
                member: balancer.as_ref().map(|_| outbound.clone()),
                outbound,
                balancer,
            },
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ProductFlowMetadata {
    pub display_id: u64,
    pub session_id: Option<SessionId>,
    pub flow_id: ProductFlowId,
    pub network: Network,
    /// `None` denotes a reusable product association that can reach many targets.
    pub target: Option<TargetAddr>,
    pub scope: Option<Arc<ProductFlowScope>>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct ProductIoSnapshot {
    pub to_peer_bytes: u64,
    pub to_peer_packets: u64,
    pub from_peer_bytes: u64,
    pub from_peer_packets: u64,
}

impl ProductIoSnapshot {
    fn combined(self, other: Self) -> Self {
        Self {
            to_peer_bytes: self.to_peer_bytes.saturating_add(other.to_peer_bytes),
            to_peer_packets: self.to_peer_packets.saturating_add(other.to_peer_packets),
            from_peer_bytes: self.from_peer_bytes.saturating_add(other.from_peer_bytes),
            from_peer_packets: self
                .from_peer_packets
                .saturating_add(other.from_peer_packets),
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct ProductFlowLifecycleSnapshot {
    pub opened: u64,
    pub active: u64,
    pub completed: u64,
    pub failed: u64,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct ReliableTelemetrySnapshot {
    pub io: ProductIoSnapshot,
    pub flows: ProductFlowLifecycleSnapshot,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct DatagramTelemetrySnapshot {
    pub io: ProductIoSnapshot,
    pub flows: ProductFlowLifecycleSnapshot,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ActiveProductFlowSnapshot {
    pub display_id: u64,
    pub session_id: Option<SessionId>,
    pub flow_id: ProductFlowId,
    pub network: Network,
    pub target: Option<TargetAddr>,
    pub origin: Option<ProductFlowOrigin>,
    pub selection: Option<ProductFlowSelection>,
    pub started_at: Instant,
    pub last_activity_at: Instant,
    pub io: ProductIoSnapshot,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RuntimeTelemetrySnapshot {
    pub observed_at: Instant,
    pub io: ProductIoSnapshot,
    pub reliable: ReliableTelemetrySnapshot,
    pub datagram: DatagramTelemetrySnapshot,
    pub active_flow_capacity: usize,
    pub active_flow_record_overflow: u64,
    pub active_flow_record_overflow_total: u64,
    pub active_flows: Vec<ActiveProductFlowSnapshot>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ProductFlowKind {
    Reliable,
    Datagram,
}

#[derive(Debug, Default)]
struct ProductIoCounters {
    to_peer_bytes: AtomicU64,
    to_peer_packets: AtomicU64,
    from_peer_bytes: AtomicU64,
    from_peer_packets: AtomicU64,
}

impl ProductIoCounters {
    fn record_to_peer(&self, bytes: u64, packets: u64) {
        self.to_peer_bytes.fetch_add(bytes, Ordering::Relaxed);
        self.to_peer_packets.fetch_add(packets, Ordering::Relaxed);
    }

    fn record_from_peer(&self, bytes: u64, packets: u64) {
        self.from_peer_bytes.fetch_add(bytes, Ordering::Relaxed);
        self.from_peer_packets.fetch_add(packets, Ordering::Relaxed);
    }

    fn snapshot(&self) -> ProductIoSnapshot {
        ProductIoSnapshot {
            to_peer_bytes: self.to_peer_bytes.load(Ordering::Relaxed),
            to_peer_packets: self.to_peer_packets.load(Ordering::Relaxed),
            from_peer_bytes: self.from_peer_bytes.load(Ordering::Relaxed),
            from_peer_packets: self.from_peer_packets.load(Ordering::Relaxed),
        }
    }
}

#[derive(Debug, Default)]
struct ProductFlowLifecycleCounters {
    opened: AtomicU64,
    active: AtomicU64,
    completed: AtomicU64,
    failed: AtomicU64,
}

impl ProductFlowLifecycleCounters {
    fn open(&self) {
        self.opened.fetch_add(1, Ordering::Relaxed);
        self.active.fetch_add(1, Ordering::Relaxed);
    }

    fn finish(&self, completed: bool) {
        self.active.fetch_sub(1, Ordering::Relaxed);
        if completed {
            self.completed.fetch_add(1, Ordering::Relaxed);
        } else {
            self.failed.fetch_add(1, Ordering::Relaxed);
        }
    }

    fn snapshot(&self) -> ProductFlowLifecycleSnapshot {
        ProductFlowLifecycleSnapshot {
            opened: self.opened.load(Ordering::Relaxed),
            active: self.active.load(Ordering::Relaxed),
            completed: self.completed.load(Ordering::Relaxed),
            failed: self.failed.load(Ordering::Relaxed),
        }
    }
}

#[derive(Debug)]
struct ActiveProductFlow {
    metadata: ProductFlowMetadata,
    started_elapsed_nanos: u64,
    last_activity_elapsed_nanos: AtomicU64,
    io: ProductIoCounters,
    flow_log: OnceLock<crate::observability::FlowLogToken>,
}

#[derive(Debug)]
struct OverflowFlowLog {
    flow_id: u64,
    network: Network,
    started_elapsed_nanos: u64,
    io: ProductIoCounters,
    token: crate::observability::FlowLogToken,
}

impl ActiveProductFlow {
    fn new(metadata: ProductFlowMetadata, started_elapsed_nanos: u64) -> Self {
        Self {
            metadata,
            started_elapsed_nanos,
            last_activity_elapsed_nanos: AtomicU64::new(started_elapsed_nanos),
            io: ProductIoCounters::default(),
            flow_log: OnceLock::new(),
        }
    }

    fn record_activity(&self, elapsed_nanos: u64) {
        self.last_activity_elapsed_nanos
            .fetch_max(elapsed_nanos, Ordering::Relaxed);
    }
}

fn flow_network_label(network: Network) -> &'static str {
    match network {
        Network::Tcp => "tcp",
        Network::Udp => "udp",
    }
}

fn emit_flow_open(metadata: &ProductFlowMetadata) -> Option<crate::observability::FlowLogToken> {
    if !crate::observability::flow_events_enabled() {
        return None;
    }
    let scope = metadata.scope.as_ref()?;
    let origin = match scope.origin.kind {
        ProductFlowOriginKind::LocalInbound => "local_inbound",
        ProductFlowOriginKind::MppInbound => "mpp_inbound",
    };
    let target = scope.target.authority();
    crate::observability::emit_flow_open(
        metadata.display_id,
        origin,
        flow_network_label(metadata.network),
        scope.origin.inbound.as_str(),
        &target,
        scope.selection.outbound.as_str(),
        scope.selection.balancer.as_ref().map(BalancerId::as_str),
    )
}

fn emit_flow_close(flow: &ActiveProductFlow, elapsed_nanos: u64, completed: bool) {
    let Some(token) = flow.flow_log.get() else {
        return;
    };
    let io = flow.io.snapshot();
    crate::observability::emit_flow_close(
        token,
        flow.metadata.display_id,
        flow_network_label(flow.metadata.network),
        if completed { "complete" } else { "incomplete" },
        elapsed_nanos
            .saturating_sub(flow.started_elapsed_nanos)
            .saturating_div(1_000_000),
        crate::observability::FlowIo {
            to_peer_bytes: io.to_peer_bytes,
            to_peer_packets: io.to_peer_packets,
            from_peer_bytes: io.from_peer_bytes,
            from_peer_packets: io.from_peer_packets,
        },
    );
}

fn emit_overflow_flow_close(flow: &OverflowFlowLog, elapsed_nanos: u64, completed: bool) {
    let io = flow.io.snapshot();
    crate::observability::emit_flow_close(
        &flow.token,
        flow.flow_id,
        flow_network_label(flow.network),
        if completed { "complete" } else { "incomplete" },
        elapsed_nanos
            .saturating_sub(flow.started_elapsed_nanos)
            .saturating_div(1_000_000),
        crate::observability::FlowIo {
            to_peer_bytes: io.to_peer_bytes,
            to_peer_packets: io.to_peer_packets,
            from_peer_bytes: io.from_peer_bytes,
            from_peer_packets: io.from_peer_packets,
        },
    );
}

#[derive(Debug)]
struct RuntimeTelemetryInner {
    started_at: Instant,
    active_flow_capacity: usize,
    #[cfg(test)]
    next_local_datagram_flow_id: AtomicU64,
    next_display_id: AtomicU64,
    next_registration_id: AtomicU64,
    active_flow_record_overflow: AtomicU64,
    active_flow_record_overflow_total: AtomicU64,
    active_flows: Mutex<HashMap<u64, Arc<ActiveProductFlow>>>,
    reliable_io: ProductIoCounters,
    reliable_flows: ProductFlowLifecycleCounters,
    datagram_io: ProductIoCounters,
    datagram_flows: ProductFlowLifecycleCounters,
}

impl RuntimeTelemetryInner {
    fn elapsed_nanos(&self) -> u64 {
        self.started_at.elapsed().as_nanos().min(u64::MAX as u128) as u64
    }

    fn instant_at(&self, elapsed_nanos: u64) -> Instant {
        self.started_at
            .checked_add(Duration::from_nanos(elapsed_nanos))
            .unwrap_or(self.started_at)
    }

    fn io(&self, kind: ProductFlowKind) -> &ProductIoCounters {
        match kind {
            ProductFlowKind::Reliable => &self.reliable_io,
            ProductFlowKind::Datagram => &self.datagram_io,
        }
    }

    fn lifecycle(&self, kind: ProductFlowKind) -> &ProductFlowLifecycleCounters {
        match kind {
            ProductFlowKind::Reliable => &self.reliable_flows,
            ProductFlowKind::Datagram => &self.datagram_flows,
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct RuntimeTelemetry {
    inner: Arc<RuntimeTelemetryInner>,
    scope: Option<Arc<ProductFlowScope>>,
    observe_unscoped: bool,
}

impl RuntimeTelemetry {
    pub(crate) fn new(active_flow_capacity: usize) -> Self {
        Self::new_inner(active_flow_capacity, true)
    }

    /// Creates the single Product telemetry owner for one runtime generation.
    ///
    /// Its unscoped handle is intentionally inert: internal DNS/probe traffic
    /// can reuse MPP contexts without entering Product totals. Concrete routed
    /// flows enable observation by cloning a scoped handle.
    pub(crate) fn generation_owner(active_flow_capacity: usize) -> Self {
        Self::new_inner(active_flow_capacity, false)
    }

    fn new_inner(active_flow_capacity: usize, observe_unscoped: bool) -> Self {
        Self {
            inner: Arc::new(RuntimeTelemetryInner {
                started_at: Instant::now(),
                active_flow_capacity,
                #[cfg(test)]
                next_local_datagram_flow_id: AtomicU64::new(0),
                next_display_id: AtomicU64::new(1),
                next_registration_id: AtomicU64::new(1),
                active_flow_record_overflow: AtomicU64::new(0),
                active_flow_record_overflow_total: AtomicU64::new(0),
                active_flows: Mutex::new(HashMap::new()),
                reliable_io: ProductIoCounters::default(),
                reliable_flows: ProductFlowLifecycleCounters::default(),
                datagram_io: ProductIoCounters::default(),
                datagram_flows: ProductFlowLifecycleCounters::default(),
            }),
            scope: None,
            observe_unscoped,
        }
    }

    pub(crate) fn scoped(&self, scope: ProductFlowScope) -> Self {
        Self {
            inner: self.inner.clone(),
            scope: Some(Arc::new(scope)),
            observe_unscoped: false,
        }
    }

    pub(crate) fn open_reliable_flow(
        &self,
        session_id: Option<SessionId>,
        stream_id: StreamId,
        target: TargetAddr,
    ) -> ProductFlowLease {
        self.open_flow(session_id, ProductFlowId::Reliable(stream_id), Some(target))
    }

    pub(crate) fn open_datagram_flow(
        &self,
        session_id: Option<SessionId>,
        flow_id: DatagramFlowId,
        target: TargetAddr,
    ) -> ProductFlowLease {
        self.open_flow(session_id, ProductFlowId::Datagram(flow_id), Some(target))
    }

    pub(crate) fn open_native_reliable_flow(&self, scope: ProductFlowScope) -> ProductFlowLease {
        self.scoped(scope)
            .open_flow(None, ProductFlowId::NativeReliable, None)
    }

    pub(crate) fn open_native_datagram_flow(&self, scope: ProductFlowScope) -> ProductFlowLease {
        self.scoped(scope)
            .open_flow(None, ProductFlowId::NativeDatagram, None)
    }

    /// Local display identities avoid adding entropy or I/O failure to forwarding.
    #[cfg(test)]
    pub(crate) fn open_local_datagram_flow(
        &self,
        session_id: Option<SessionId>,
    ) -> ProductFlowLease {
        let flow_id = DatagramFlowId(
            self.inner
                .next_local_datagram_flow_id
                .fetch_add(1, Ordering::Relaxed),
        );
        self.open_flow(session_id, ProductFlowId::Datagram(flow_id), None)
    }

    fn open_flow(
        &self,
        session_id: Option<SessionId>,
        flow_id: ProductFlowId,
        target: Option<TargetAddr>,
    ) -> ProductFlowLease {
        let kind = flow_id.kind();
        let enabled = self.observe_unscoped || self.scope.is_some();
        if !enabled {
            return ProductFlowLease {
                counter: ProductFlowCounter {
                    telemetry: self.clone(),
                    flow: None,
                    overflow_flow_log: None,
                    kind,
                    enabled: false,
                },
                registration_id: None,
                observed: false,
                finished: false,
            };
        }
        let display_id = self.inner.next_display_id.fetch_add(1, Ordering::Relaxed);
        let network = match kind {
            ProductFlowKind::Reliable => Network::Tcp,
            ProductFlowKind::Datagram => Network::Udp,
        };
        let metadata = ProductFlowMetadata {
            display_id,
            session_id,
            flow_id,
            network,
            target: self
                .scope
                .as_ref()
                .map(|scope| scope.target.clone())
                .or(target),
            scope: self.scope.clone(),
        };
        debug_assert!(
            metadata
                .scope
                .as_ref()
                .is_none_or(|scope| scope.network == network)
        );
        let started_elapsed_nanos = self.inner.elapsed_nanos();
        let (flow, registration_id, overflow_metadata) = {
            let mut active_flows = self
                .inner
                .active_flows
                .lock()
                .expect("runtime telemetry flow registry poisoned");
            if active_flows.len() < self.inner.active_flow_capacity {
                let registration_id = self
                    .inner
                    .next_registration_id
                    .fetch_add(1, Ordering::Relaxed);
                let flow = Arc::new(ActiveProductFlow::new(metadata, started_elapsed_nanos));
                active_flows.insert(registration_id, flow.clone());
                (Some(flow), Some(registration_id), None)
            } else {
                // Overflow flows keep aggregate counters without per-flow state.
                self.inner
                    .active_flow_record_overflow
                    .fetch_add(1, Ordering::Relaxed);
                self.inner
                    .active_flow_record_overflow_total
                    .fetch_add(1, Ordering::Relaxed);
                (None, None, Some(metadata))
            }
        };
        self.inner.lifecycle(kind).open();
        if let Some(flow) = flow.as_ref()
            && let Some(token) = emit_flow_open(&flow.metadata)
        {
            flow.flow_log
                .set(token)
                .expect("flow log token is initialized once");
        }
        let overflow_flow_log = overflow_metadata.and_then(|metadata| {
            metadata.scope.as_ref()?;
            let token = emit_flow_open(&metadata)?;
            Some(Arc::new(OverflowFlowLog {
                flow_id: metadata.display_id,
                network: metadata.network,
                started_elapsed_nanos,
                io: ProductIoCounters::default(),
                token,
            }))
        });

        ProductFlowLease {
            counter: ProductFlowCounter {
                telemetry: self.clone(),
                flow,
                overflow_flow_log,
                kind,
                enabled: true,
            },
            registration_id,
            observed: true,
            finished: false,
        }
    }

    pub(crate) fn snapshot(&self) -> RuntimeTelemetrySnapshot {
        let reliable = ReliableTelemetrySnapshot {
            io: self.inner.reliable_io.snapshot(),
            flows: self.inner.reliable_flows.snapshot(),
        };
        let datagram = DatagramTelemetrySnapshot {
            io: self.inner.datagram_io.snapshot(),
            flows: self.inner.datagram_flows.snapshot(),
        };
        let mut registered_flows = {
            let active_flows = self
                .inner
                .active_flows
                .lock()
                .expect("runtime telemetry flow registry poisoned");
            active_flows
                .iter()
                .map(|(registration_id, flow)| (*registration_id, Arc::clone(flow)))
                .collect::<Vec<_>>()
        };
        registered_flows.sort_unstable_by_key(|(registration_id, _)| *registration_id);
        let active_flows = registered_flows
            .into_iter()
            .map(|(_, flow)| {
                let last_activity_elapsed_nanos =
                    flow.last_activity_elapsed_nanos.load(Ordering::Relaxed);
                ActiveProductFlowSnapshot {
                    display_id: flow.metadata.display_id,
                    session_id: flow.metadata.session_id,
                    flow_id: flow.metadata.flow_id,
                    network: flow.metadata.network,
                    target: flow.metadata.target.clone(),
                    origin: flow
                        .metadata
                        .scope
                        .as_ref()
                        .map(|scope| scope.origin.clone()),
                    selection: flow
                        .metadata
                        .scope
                        .as_ref()
                        .map(|scope| scope.selection.clone()),
                    started_at: self.inner.instant_at(flow.started_elapsed_nanos),
                    last_activity_at: self.inner.instant_at(last_activity_elapsed_nanos),
                    io: flow.io.snapshot(),
                }
            })
            .collect();

        RuntimeTelemetrySnapshot {
            observed_at: Instant::now(),
            io: reliable.io.combined(datagram.io),
            reliable,
            datagram,
            active_flow_capacity: self.inner.active_flow_capacity,
            active_flow_record_overflow: self
                .inner
                .active_flow_record_overflow
                .load(Ordering::Relaxed),
            active_flow_record_overflow_total: self
                .inner
                .active_flow_record_overflow_total
                .load(Ordering::Relaxed),
            active_flows,
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct ProductFlowCounter {
    telemetry: RuntimeTelemetry,
    flow: Option<Arc<ActiveProductFlow>>,
    overflow_flow_log: Option<Arc<OverflowFlowLog>>,
    kind: ProductFlowKind,
    enabled: bool,
}

impl ProductFlowCounter {
    pub(crate) fn record_to_peer_bytes(&self, bytes: u64) {
        if bytes == 0 {
            return;
        }
        self.record_to_peer(bytes, 0);
    }

    pub(crate) fn record_from_peer_bytes(&self, bytes: u64) {
        if bytes == 0 {
            return;
        }
        self.record_from_peer(bytes, 0);
    }

    pub(crate) fn record_datagram_to_peer(&self, payload_bytes: u64) {
        debug_assert_eq!(self.kind, ProductFlowKind::Datagram);
        self.record_to_peer(payload_bytes, 1);
    }

    pub(crate) fn record_datagram_from_peer(&self, payload_bytes: u64) {
        debug_assert_eq!(self.kind, ProductFlowKind::Datagram);
        self.record_from_peer(payload_bytes, 1);
    }

    fn record_to_peer(&self, bytes: u64, packets: u64) {
        if !self.enabled {
            return;
        }
        self.telemetry
            .inner
            .io(self.kind)
            .record_to_peer(bytes, packets);
        if let Some(flow) = &self.flow {
            flow.io.record_to_peer(bytes, packets);
            flow.record_activity(self.telemetry.inner.elapsed_nanos());
        } else if let Some(flow) = &self.overflow_flow_log {
            flow.io.record_to_peer(bytes, packets);
        }
    }

    fn record_from_peer(&self, bytes: u64, packets: u64) {
        if !self.enabled {
            return;
        }
        self.telemetry
            .inner
            .io(self.kind)
            .record_from_peer(bytes, packets);
        if let Some(flow) = &self.flow {
            flow.io.record_from_peer(bytes, packets);
            flow.record_activity(self.telemetry.inner.elapsed_nanos());
        } else if let Some(flow) = &self.overflow_flow_log {
            flow.io.record_from_peer(bytes, packets);
        }
    }
}

#[derive(Debug)]
pub(crate) struct ProductFlowLease {
    counter: ProductFlowCounter,
    registration_id: Option<u64>,
    observed: bool,
    finished: bool,
}

impl ProductFlowLease {
    pub(crate) fn counter(&self) -> ProductFlowCounter {
        self.counter.clone()
    }

    pub(crate) fn complete(mut self) {
        self.finish(true);
    }

    fn finish(&mut self, completed: bool) {
        if self.finished {
            return;
        }
        self.finished = true;
        if !self.observed {
            return;
        }
        if let Some(registration_id) = self.registration_id.take() {
            self.counter
                .telemetry
                .inner
                .active_flows
                .lock()
                .expect("runtime telemetry flow registry poisoned")
                .remove(&registration_id);
        } else {
            self.counter
                .telemetry
                .inner
                .active_flow_record_overflow
                .fetch_sub(1, Ordering::Relaxed);
        }
        self.counter
            .telemetry
            .inner
            .lifecycle(self.counter.kind)
            .finish(completed);
        if let Some(flow) = self.counter.flow.as_ref() {
            emit_flow_close(
                flow,
                self.counter.telemetry.inner.elapsed_nanos(),
                completed,
            );
        } else if let Some(flow) = self.counter.overflow_flow_log.as_ref() {
            emit_overflow_flow_close(
                flow,
                self.counter.telemetry.inner.elapsed_nanos(),
                completed,
            );
        }
    }
}

impl Drop for ProductFlowLease {
    fn drop(&mut self) {
        self.finish(false);
    }
}

#[derive(Debug)]
pub(crate) struct ObservedProductIo<S> {
    inner: S,
    counter: ProductFlowCounter,
}

impl<S> ObservedProductIo<S> {
    pub(crate) fn new(inner: S, counter: ProductFlowCounter) -> Self {
        Self { inner, counter }
    }
}

impl<S> AsyncRead for ObservedProductIo<S>
where
    S: AsyncRead + Unpin,
{
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        let filled_before = buf.filled().len();
        let result = Pin::new(&mut this.inner).poll_read(cx, buf);
        if let Poll::Ready(Ok(())) = &result {
            let bytes = buf.filled().len().saturating_sub(filled_before) as u64;
            this.counter.record_to_peer_bytes(bytes);
        }
        result
    }
}

impl<S> AsyncWrite for ObservedProductIo<S>
where
    S: AsyncWrite + Unpin,
{
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize, io::Error>> {
        let this = self.get_mut();
        let result = Pin::new(&mut this.inner).poll_write(cx, buf);
        if let Poll::Ready(Ok(bytes)) = &result {
            this.counter.record_from_peer_bytes(*bytes as u64);
        }
        result
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), io::Error>> {
        Pin::new(&mut self.get_mut().inner).poll_flush(cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), io::Error>> {
        Pin::new(&mut self.get_mut().inner).poll_shutdown(cx)
    }

    fn poll_write_vectored(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[io::IoSlice<'_>],
    ) -> Poll<Result<usize, io::Error>> {
        let this = self.get_mut();
        let result = Pin::new(&mut this.inner).poll_write_vectored(cx, bufs);
        if let Poll::Ready(Ok(bytes)) = &result {
            this.counter.record_from_peer_bytes(*bytes as u64);
        }
        result
    }

    fn is_write_vectored(&self) -> bool {
        self.inner.is_write_vectored()
    }
}

#[cfg(test)]
#[path = "tests_telemetry.rs"]
mod tests;
