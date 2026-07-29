//! Generation-owned Product resource admission.
//!
//! This owner is intentionally independent of MPP carrier, session, stream,
//! datagram, queue, and scheduler limits. Admission happens only while a
//! Product flow or DNS lookup is opened; retained permits perform no payload
//! work.

use super::{OutboundId, PrincipalId, ProtocolTarget};
use std::collections::{HashMap, hash_map::Entry};
use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

pub const DEFAULT_MAX_PRODUCT_LIVE_FLOWS: usize = 4_096;
pub const DEFAULT_MAX_PRODUCT_CONCURRENT_WORK: usize = 512;
pub const DEFAULT_MAX_PRODUCT_LIVE_FLOWS_PER_PRINCIPAL: usize = 1_024;
pub const DEFAULT_MAX_PRODUCT_LIVE_FLOWS_PER_OUTBOUND: usize = 3_072;
pub const DEFAULT_MAX_PRODUCT_CONNECTS_PER_OUTBOUND: usize = 256;
pub const DEFAULT_MAX_PRODUCT_LIVE_FLOWS_PER_TARGET: usize = 256;
pub const DEFAULT_MAX_PRODUCT_CONNECTS_PER_TARGET: usize = 32;
pub const DEFAULT_MAX_PRODUCT_DNS_WORK: usize = 128;
pub const MAX_PRODUCT_ADMISSION_LIMIT: usize = 1_000_000;

/// Process-wide Product resource limits for one immutable runtime generation.
///
/// A connect is also live Product work and counts against the outbound's live
/// flow bound until it fails or becomes an established flow.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProductAdmissionConfig {
    pub max_live_flows: usize,
    pub max_concurrent_work: usize,
    pub max_live_flows_per_principal: usize,
    pub max_live_flows_per_outbound: usize,
    pub max_connects_per_outbound: usize,
    pub max_live_flows_per_target: usize,
    pub max_connects_per_target: usize,
    pub max_dns_work: usize,
}

impl ProductAdmissionConfig {
    pub fn validate(self) -> Result<(), ProductAdmissionConfigError> {
        for (field, value) in [
            ("max_live_flows", self.max_live_flows),
            ("max_concurrent_work", self.max_concurrent_work),
            (
                "max_live_flows_per_principal",
                self.max_live_flows_per_principal,
            ),
            (
                "max_live_flows_per_outbound",
                self.max_live_flows_per_outbound,
            ),
            ("max_connects_per_outbound", self.max_connects_per_outbound),
            ("max_live_flows_per_target", self.max_live_flows_per_target),
            ("max_connects_per_target", self.max_connects_per_target),
            ("max_dns_work", self.max_dns_work),
        ] {
            if value == 0 {
                return Err(ProductAdmissionConfigError::Zero { field });
            }
            if value > MAX_PRODUCT_ADMISSION_LIMIT {
                return Err(ProductAdmissionConfigError::TooLarge {
                    field,
                    value,
                    maximum: MAX_PRODUCT_ADMISSION_LIMIT,
                });
            }
        }
        for (field, value) in [
            (
                "max_live_flows_per_principal",
                self.max_live_flows_per_principal,
            ),
            (
                "max_live_flows_per_outbound",
                self.max_live_flows_per_outbound,
            ),
            ("max_live_flows_per_target", self.max_live_flows_per_target),
        ] {
            if value > self.max_live_flows {
                return Err(ProductAdmissionConfigError::ExceedsGlobal {
                    field,
                    value,
                    global_field: "max_live_flows",
                    global: self.max_live_flows,
                });
            }
        }
        for (field, value) in [
            ("max_connects_per_outbound", self.max_connects_per_outbound),
            ("max_connects_per_target", self.max_connects_per_target),
            ("max_dns_work", self.max_dns_work),
        ] {
            if value > self.max_concurrent_work {
                return Err(ProductAdmissionConfigError::ExceedsGlobal {
                    field,
                    value,
                    global_field: "max_concurrent_work",
                    global: self.max_concurrent_work,
                });
            }
        }
        if self.max_connects_per_outbound > self.max_live_flows_per_outbound {
            return Err(ProductAdmissionConfigError::ExceedsGlobal {
                field: "max_connects_per_outbound",
                value: self.max_connects_per_outbound,
                global_field: "max_live_flows_per_outbound",
                global: self.max_live_flows_per_outbound,
            });
        }
        if self.max_connects_per_target > self.max_live_flows_per_target {
            return Err(ProductAdmissionConfigError::ExceedsGlobal {
                field: "max_connects_per_target",
                value: self.max_connects_per_target,
                global_field: "max_live_flows_per_target",
                global: self.max_live_flows_per_target,
            });
        }
        Ok(())
    }
}

impl Default for ProductAdmissionConfig {
    fn default() -> Self {
        Self {
            max_live_flows: DEFAULT_MAX_PRODUCT_LIVE_FLOWS,
            max_concurrent_work: DEFAULT_MAX_PRODUCT_CONCURRENT_WORK,
            max_live_flows_per_principal: DEFAULT_MAX_PRODUCT_LIVE_FLOWS_PER_PRINCIPAL,
            max_live_flows_per_outbound: DEFAULT_MAX_PRODUCT_LIVE_FLOWS_PER_OUTBOUND,
            max_connects_per_outbound: DEFAULT_MAX_PRODUCT_CONNECTS_PER_OUTBOUND,
            max_live_flows_per_target: DEFAULT_MAX_PRODUCT_LIVE_FLOWS_PER_TARGET,
            max_connects_per_target: DEFAULT_MAX_PRODUCT_CONNECTS_PER_TARGET,
            max_dns_work: DEFAULT_MAX_PRODUCT_DNS_WORK,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProductAdmissionConfigError {
    Zero {
        field: &'static str,
    },
    TooLarge {
        field: &'static str,
        value: usize,
        maximum: usize,
    },
    ExceedsGlobal {
        field: &'static str,
        value: usize,
        global_field: &'static str,
        global: usize,
    },
}

impl fmt::Display for ProductAdmissionConfigError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Zero { field } => write!(formatter, "Product admission {field} must be non-zero"),
            Self::TooLarge {
                field,
                value,
                maximum,
            } => write!(
                formatter,
                "Product admission {field} is {value}; maximum is {maximum}"
            ),
            Self::ExceedsGlobal {
                field,
                value,
                global_field,
                global,
            } => write!(
                formatter,
                "Product admission {field} ({value}) exceeds {global_field} ({global})"
            ),
        }
    }
}

impl std::error::Error for ProductAdmissionConfigError {}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProductAdmissionRejection {
    GlobalLiveFlows,
    PrincipalLiveFlows,
    OutboundLiveFlows,
    TargetLiveFlows,
    GlobalConcurrentWork,
    OutboundConnects,
    TargetConnects,
    DnsWork,
}

impl ProductAdmissionRejection {
    const COUNT: usize = 8;

    const fn index(self) -> usize {
        match self {
            Self::GlobalLiveFlows => 0,
            Self::PrincipalLiveFlows => 1,
            Self::OutboundLiveFlows => 2,
            Self::TargetLiveFlows => 3,
            Self::GlobalConcurrentWork => 4,
            Self::OutboundConnects => 5,
            Self::TargetConnects => 6,
            Self::DnsWork => 7,
        }
    }

    const fn message(self) -> &'static str {
        match self {
            Self::GlobalLiveFlows => "global live-flow limit reached",
            Self::PrincipalLiveFlows => "principal live-flow limit reached",
            Self::OutboundLiveFlows => "outbound live-flow limit reached",
            Self::TargetLiveFlows => "target live-flow limit reached",
            Self::GlobalConcurrentWork => "global concurrent-work limit reached",
            Self::OutboundConnects => "outbound connect limit reached",
            Self::TargetConnects => "target connect limit reached",
            Self::DnsWork => "DNS concurrent-work limit reached",
        }
    }
}

impl fmt::Display for ProductAdmissionRejection {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.message())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProductAdmissionError {
    rejection: ProductAdmissionRejection,
}

impl ProductAdmissionError {
    pub const fn rejection(self) -> ProductAdmissionRejection {
        self.rejection
    }
}

impl fmt::Display for ProductAdmissionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.rejection.message())
    }
}

impl std::error::Error for ProductAdmissionError {}

#[derive(Clone)]
pub struct ProductAdmission {
    inner: Arc<ProductAdmissionInner>,
}

struct ProductAdmissionInner {
    owner_generation: u64,
    limits: ProductAdmissionConfig,
    state: Mutex<ProductAdmissionState>,
}

#[derive(Default)]
struct ProductAdmissionState {
    live_flows: usize,
    concurrent_work: usize,
    dns_work: usize,
    principal_live: HashMap<PrincipalId, usize>,
    outbound_live: HashMap<OutboundId, usize>,
    outbound_connecting: HashMap<OutboundId, usize>,
    target_live: HashMap<ProtocolTarget, usize>,
    target_connecting: HashMap<ProtocolTarget, usize>,
    rejections: [u64; ProductAdmissionRejection::COUNT],
}

impl fmt::Debug for ProductAdmission {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ProductAdmission")
            .field("owner_generation", &self.inner.owner_generation)
            .field("limits", &self.inner.limits)
            .finish_non_exhaustive()
    }
}

impl ProductAdmission {
    pub fn new(limits: ProductAdmissionConfig) -> Result<Self, ProductAdmissionConfigError> {
        static NEXT_OWNER_GENERATION: AtomicU64 = AtomicU64::new(1);

        limits.validate()?;
        let owner_generation = NEXT_OWNER_GENERATION.fetch_add(1, Ordering::Relaxed);
        Ok(Self {
            inner: Arc::new(ProductAdmissionInner {
                owner_generation,
                limits,
                state: Mutex::new(ProductAdmissionState::default()),
            }),
        })
    }

    pub fn limits(&self) -> ProductAdmissionConfig {
        self.inner.limits
    }

    pub fn try_admit_flow(
        &self,
        principal: PrincipalId,
        target: ProtocolTarget,
    ) -> Result<PendingProductFlow, ProductAdmissionError> {
        let mut state = self.state();
        if state.live_flows >= self.inner.limits.max_live_flows {
            return Err(reject(
                &mut state,
                ProductAdmissionRejection::GlobalLiveFlows,
            ));
        }
        if state.principal_live.get(&principal).copied().unwrap_or(0)
            >= self.inner.limits.max_live_flows_per_principal
        {
            return Err(reject(
                &mut state,
                ProductAdmissionRejection::PrincipalLiveFlows,
            ));
        }
        if state.target_live.get(&target).copied().unwrap_or(0)
            >= self.inner.limits.max_live_flows_per_target
        {
            return Err(reject(
                &mut state,
                ProductAdmissionRejection::TargetLiveFlows,
            ));
        }
        state.live_flows += 1;
        *state.principal_live.entry(principal.clone()).or_default() += 1;
        *state.target_live.entry(target.clone()).or_default() += 1;
        Ok(PendingProductFlow {
            admission: self.clone(),
            principal,
            target,
            active: true,
        })
    }

    pub fn try_admit_dns_work(&self) -> Result<ProductDnsWork, ProductAdmissionError> {
        let mut state = self.state();
        if state.concurrent_work >= self.inner.limits.max_concurrent_work {
            return Err(reject(
                &mut state,
                ProductAdmissionRejection::GlobalConcurrentWork,
            ));
        }
        if state.dns_work >= self.inner.limits.max_dns_work {
            return Err(reject(&mut state, ProductAdmissionRejection::DnsWork));
        }
        state.concurrent_work += 1;
        state.dns_work += 1;
        Ok(ProductDnsWork {
            admission: self.clone(),
            active: true,
        })
    }

    pub fn snapshot(&self) -> ProductAdmissionSnapshot {
        let state = self.state();
        let mut principals = state
            .principal_live
            .iter()
            .map(|(principal, live)| ProductPrincipalAdmissionSnapshot {
                principal: principal.clone(),
                live_flows: *live,
            })
            .collect::<Vec<_>>();
        principals.sort_unstable_by(|left, right| left.principal.cmp(&right.principal));
        let mut outbounds = state
            .outbound_live
            .iter()
            .map(|(outbound, live)| ProductOutboundAdmissionSnapshot {
                outbound: outbound.clone(),
                live_flows: *live,
                connecting: state
                    .outbound_connecting
                    .get(outbound)
                    .copied()
                    .unwrap_or(0),
            })
            .collect::<Vec<_>>();
        outbounds.sort_unstable_by(|left, right| left.outbound.cmp(&right.outbound));
        let mut targets = state
            .target_live
            .iter()
            .map(|(target, live)| ProductTargetAdmissionSnapshot {
                target: target.clone(),
                live_flows: *live,
                connecting: state.target_connecting.get(target).copied().unwrap_or(0),
            })
            .collect::<Vec<_>>();
        targets.sort_unstable_by_key(|entry| entry.target.authority());
        ProductAdmissionSnapshot {
            owner_generation: self.inner.owner_generation,
            limits: self.inner.limits,
            live_flows: state.live_flows,
            concurrent_work: state.concurrent_work,
            dns_work: state.dns_work,
            principals,
            outbounds,
            targets,
            rejections: ProductAdmissionRejectionSnapshot {
                global_live_flows: state.rejections
                    [ProductAdmissionRejection::GlobalLiveFlows.index()],
                principal_live_flows: state.rejections
                    [ProductAdmissionRejection::PrincipalLiveFlows.index()],
                outbound_live_flows: state.rejections
                    [ProductAdmissionRejection::OutboundLiveFlows.index()],
                target_live_flows: state.rejections
                    [ProductAdmissionRejection::TargetLiveFlows.index()],
                global_concurrent_work: state.rejections
                    [ProductAdmissionRejection::GlobalConcurrentWork.index()],
                outbound_connects: state.rejections
                    [ProductAdmissionRejection::OutboundConnects.index()],
                target_connects: state.rejections
                    [ProductAdmissionRejection::TargetConnects.index()],
                dns_work: state.rejections[ProductAdmissionRejection::DnsWork.index()],
            },
        }
    }

    fn state(&self) -> std::sync::MutexGuard<'_, ProductAdmissionState> {
        self.inner
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

impl Default for ProductAdmission {
    fn default() -> Self {
        Self::new(ProductAdmissionConfig::default())
            .expect("default Product admission limits are valid")
    }
}

pub struct PendingProductFlow {
    admission: ProductAdmission,
    principal: PrincipalId,
    target: ProtocolTarget,
    active: bool,
}

impl fmt::Debug for PendingProductFlow {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PendingProductFlow")
            .field("owner_generation", &self.admission.inner.owner_generation)
            .field("principal", &self.principal)
            .field("target", &self.target)
            .field("active", &self.active)
            .finish()
    }
}

impl PendingProductFlow {
    pub const fn principal(&self) -> &PrincipalId {
        &self.principal
    }

    pub const fn target(&self) -> &ProtocolTarget {
        &self.target
    }

    pub fn try_begin_connect(
        &self,
        outbound: OutboundId,
    ) -> Result<ProductConnectWork, ProductAdmissionError> {
        debug_assert!(self.active);
        let mut state = self.admission.state();
        if state.concurrent_work >= self.admission.inner.limits.max_concurrent_work {
            return Err(reject(
                &mut state,
                ProductAdmissionRejection::GlobalConcurrentWork,
            ));
        }
        if state.outbound_live.get(&outbound).copied().unwrap_or(0)
            >= self.admission.inner.limits.max_live_flows_per_outbound
        {
            return Err(reject(
                &mut state,
                ProductAdmissionRejection::OutboundLiveFlows,
            ));
        }
        if state
            .outbound_connecting
            .get(&outbound)
            .copied()
            .unwrap_or(0)
            >= self.admission.inner.limits.max_connects_per_outbound
        {
            return Err(reject(
                &mut state,
                ProductAdmissionRejection::OutboundConnects,
            ));
        }
        if state
            .target_connecting
            .get(&self.target)
            .copied()
            .unwrap_or(0)
            >= self.admission.inner.limits.max_connects_per_target
        {
            return Err(reject(
                &mut state,
                ProductAdmissionRejection::TargetConnects,
            ));
        }
        state.concurrent_work += 1;
        *state.outbound_live.entry(outbound.clone()).or_default() += 1;
        *state
            .outbound_connecting
            .entry(outbound.clone())
            .or_default() += 1;
        *state
            .target_connecting
            .entry(self.target.clone())
            .or_default() += 1;
        Ok(ProductConnectWork {
            admission: self.admission.clone(),
            outbound,
            target: self.target.clone(),
            active: true,
        })
    }

    pub fn commit(self, outbound: ProductOutboundFlow) -> ProductFlowLease {
        assert!(
            Arc::ptr_eq(&self.admission.inner, &outbound.admission.inner)
                && self.target == outbound.target,
            "Product flow can only commit its own generation and target"
        );
        ProductFlowLease {
            flow: self,
            outbound,
        }
    }
}

impl Drop for PendingProductFlow {
    fn drop(&mut self) {
        if !self.active {
            return;
        }
        let mut state = self.admission.state();
        decrement_exact(&mut state.live_flows);
        decrement_key_exact(&mut state.principal_live, &self.principal);
        decrement_key_exact(&mut state.target_live, &self.target);
        self.active = false;
    }
}

#[derive(Debug)]
pub struct ProductConnectWork {
    admission: ProductAdmission,
    outbound: OutboundId,
    target: ProtocolTarget,
    active: bool,
}

impl ProductConnectWork {
    pub fn connected(mut self) -> ProductOutboundFlow {
        {
            let mut state = self.admission.state();
            decrement_exact(&mut state.concurrent_work);
            decrement_key_exact(&mut state.outbound_connecting, &self.outbound);
            decrement_key_exact(&mut state.target_connecting, &self.target);
        }
        self.active = false;
        ProductOutboundFlow {
            admission: self.admission.clone(),
            outbound: self.outbound.clone(),
            target: self.target.clone(),
            active: true,
        }
    }
}

impl Drop for ProductConnectWork {
    fn drop(&mut self) {
        if !self.active {
            return;
        }
        let mut state = self.admission.state();
        decrement_exact(&mut state.concurrent_work);
        decrement_key_exact(&mut state.outbound_live, &self.outbound);
        decrement_key_exact(&mut state.outbound_connecting, &self.outbound);
        decrement_key_exact(&mut state.target_connecting, &self.target);
        self.active = false;
    }
}

#[derive(Debug)]
pub struct ProductOutboundFlow {
    admission: ProductAdmission,
    outbound: OutboundId,
    target: ProtocolTarget,
    active: bool,
}

impl Drop for ProductOutboundFlow {
    fn drop(&mut self) {
        if !self.active {
            return;
        }
        let mut state = self.admission.state();
        decrement_key_exact(&mut state.outbound_live, &self.outbound);
        self.active = false;
    }
}

/// Retained by the concrete TCP stream or UDP association for its lifetime.
///
/// The lease contains only RAII ownership and performs no per-byte or
/// per-packet accounting.
pub struct ProductFlowLease {
    flow: PendingProductFlow,
    outbound: ProductOutboundFlow,
}

impl fmt::Debug for ProductFlowLease {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ProductFlowLease")
            .field("principal", &self.flow.principal)
            .field("target", &self.flow.target)
            .field("outbound", &self.outbound.outbound)
            .finish_non_exhaustive()
    }
}

#[derive(Debug)]
pub struct ProductDnsWork {
    admission: ProductAdmission,
    active: bool,
}

impl Drop for ProductDnsWork {
    fn drop(&mut self) {
        if !self.active {
            return;
        }
        let mut state = self.admission.state();
        decrement_exact(&mut state.concurrent_work);
        decrement_exact(&mut state.dns_work);
        self.active = false;
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProductAdmissionSnapshot {
    pub owner_generation: u64,
    pub limits: ProductAdmissionConfig,
    pub live_flows: usize,
    pub concurrent_work: usize,
    pub dns_work: usize,
    pub principals: Vec<ProductPrincipalAdmissionSnapshot>,
    pub outbounds: Vec<ProductOutboundAdmissionSnapshot>,
    pub targets: Vec<ProductTargetAdmissionSnapshot>,
    pub rejections: ProductAdmissionRejectionSnapshot,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProductPrincipalAdmissionSnapshot {
    pub principal: PrincipalId,
    pub live_flows: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProductOutboundAdmissionSnapshot {
    pub outbound: OutboundId,
    pub live_flows: usize,
    pub connecting: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProductTargetAdmissionSnapshot {
    pub target: ProtocolTarget,
    pub live_flows: usize,
    pub connecting: usize,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ProductAdmissionRejectionSnapshot {
    pub global_live_flows: u64,
    pub principal_live_flows: u64,
    pub outbound_live_flows: u64,
    pub target_live_flows: u64,
    pub global_concurrent_work: u64,
    pub outbound_connects: u64,
    pub target_connects: u64,
    pub dns_work: u64,
}

fn reject(
    state: &mut ProductAdmissionState,
    rejection: ProductAdmissionRejection,
) -> ProductAdmissionError {
    let counter = &mut state.rejections[rejection.index()];
    *counter = counter.saturating_add(1);
    ProductAdmissionError { rejection }
}

fn decrement_exact(value: &mut usize) {
    debug_assert!(*value > 0, "Product admission counter underflow");
    *value = value.saturating_sub(1);
}

fn decrement_key_exact<K>(counts: &mut HashMap<K, usize>, key: &K)
where
    K: Clone + Eq + std::hash::Hash,
{
    match counts.entry(key.clone()) {
        Entry::Occupied(mut entry) if *entry.get() > 1 => {
            *entry.get_mut() -= 1;
        }
        Entry::Occupied(entry) => {
            entry.remove();
        }
        Entry::Vacant(_) => {
            debug_assert!(false, "Product admission keyed counter underflow");
        }
    }
}

#[cfg(test)]
#[path = "admission_test.rs"]
mod tests;
