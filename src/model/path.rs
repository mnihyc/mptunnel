//! Stable carrier identities shared by models and runtime services.
//!
//! A path service owns carrier lifetime and I/O; these values only identify
//! the carrier in model snapshots, intents, and exact-flight accounting.

use crate::protocol::{PathId, UnderlayProtocol};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

static NEXT_CARRIER_PATH_INSTANCE_ID: AtomicU64 = AtomicU64::new(1);

/// Endpoint-local scheduling restrictions for one configured carrier path.
///
/// These values never cross the wire: each endpoint applies the policy attached
/// to its own path configuration and publishes only resulting observations.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
pub struct PathPolicy {
    pub backup: bool,
    pub expensive: bool,
    pub bulk_allowed: bool,
    pub probe_only: bool,
    pub no_udp: bool,
}

impl Default for PathPolicy {
    fn default() -> Self {
        Self {
            backup: false,
            expensive: false,
            bulk_allowed: true,
            probe_only: false,
            no_udp: false,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct RelayPathKey {
    pub(crate) underlay: UnderlayProtocol,
    pub(crate) index: usize,
}

/// One product-stream attachment on one physical carrier lifetime.
///
/// Both identities are required: carrier evidence is invalid after reconnect,
/// while stream ownership is invalid after detach and reattach on that carrier.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct RelayPathInstance {
    pub(crate) key: RelayPathKey,
    pub(crate) path_instance_id: CarrierPathInstanceId,
    pub(crate) attachment_id: u64,
}

/// Exact path-proof authority observed for one carrier attachment.
///
/// A proof ID alone is not durable: management invalidation advances the
/// generation, while attachment time prevents evidence crossing reconnects.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct RelayPathProofEpoch {
    pub(crate) proof_id: u64,
    pub(crate) proof_generation: u64,
    pub(crate) attached_at: Instant,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct CarrierPathKey {
    pub(crate) underlay: UnderlayProtocol,
    pub(crate) path_id: PathId,
}

/// Deterministic scheduler order keeps the protocol path id primary while
/// still distinguishing equal ids carried by different transports.
pub(crate) fn carrier_path_key_order(
    left: CarrierPathKey,
    right: CarrierPathKey,
) -> std::cmp::Ordering {
    (left.path_id, left.underlay).cmp(&(right.path_id, right.underlay))
}

/// Opaque lifetime identity for one authenticated physical carrier.
///
/// `CarrierPathKey` names a logical path; this value changes when that path is
/// replaced so evidence cannot cross physical carrier lifetimes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct CarrierPathInstanceId(u64);

impl CarrierPathInstanceId {
    pub(crate) fn from_raw(raw: u64) -> Self {
        Self(raw)
    }

    pub(crate) fn as_u64(self) -> u64 {
        self.0
    }
}

/// Allocates process-unique carrier lifetime identity across all path owners.
pub(crate) fn next_carrier_path_instance_id() -> CarrierPathInstanceId {
    let instance_id = NEXT_CARRIER_PATH_INSTANCE_ID
        .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
            current.checked_add(1)
        })
        .expect("carrier path instance identity space exhausted");
    CarrierPathInstanceId::from_raw(instance_id)
}

#[cfg(test)]
#[path = "path_test.rs"]
mod tests;
