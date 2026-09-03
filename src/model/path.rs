//! Stable carrier identities shared by models and runtime services.
//!
//! A path service owns carrier lifetime and I/O; these values only identify
//! the carrier in model snapshots, intents, and exact-flight accounting.

use crate::protocol::{PathId, UnderlayProtocol};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

static NEXT_CARRIER_PATH_INSTANCE_ID: AtomicU64 = AtomicU64::new(1);

fn carrier_path_instance_identity_is_available_in(next: &AtomicU64) -> bool {
    next.load(Ordering::Acquire) != 0
}

/// Allocates one non-zero identity and permanently changes `0` into the
/// exhausted state. `u64::MAX` is issued exactly once before that transition.
fn try_allocate_carrier_path_instance_id(next: &AtomicU64) -> Option<CarrierPathInstanceId> {
    let mut current = next.load(Ordering::Acquire);
    loop {
        if current == 0 {
            return None;
        }
        let successor = current.checked_add(1).unwrap_or(0);
        match next.compare_exchange_weak(current, successor, Ordering::AcqRel, Ordering::Acquire) {
            Ok(_) => return Some(CarrierPathInstanceId::from_raw(current)),
            Err(observed) => current = observed,
        }
    }
}

/// Endpoint-local scheduling restrictions for one configured carrier path.
///
/// These values never cross the wire: each endpoint applies the policy attached
/// to its own path configuration and publishes only resulting observations.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PathPolicy {
    pub backup: bool,
    pub expensive: bool,
    pub bulk_allowed: bool,
    pub probe_only: bool,
    pub no_udp: bool,
}

impl serde::Serialize for PathPolicy {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct as _;
        let mut state = serializer.serialize_struct("PathPolicy", 5)?;
        state.serialize_field("backup", &self.backup)?;
        state.serialize_field("expensive", &self.expensive)?;
        state.serialize_field("allow_bulk", &self.bulk_allowed)?;
        state.serialize_field("control_only", &self.probe_only)?;
        state.serialize_field("allow_datagrams", &!self.no_udp)?;
        state.end()
    }
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

/// Allocates a process-unique carrier lifetime identity across all path owners.
/// Exhaustion is permanent because reusing any prior value would cross carrier
/// lifetime evidence and flight ownership.
pub(crate) fn try_next_carrier_path_instance_id() -> Option<CarrierPathInstanceId> {
    try_allocate_carrier_path_instance_id(&NEXT_CARRIER_PATH_INSTANCE_ID)
}

/// Whether a new physical carrier can still receive an exact process-wide
/// lifetime identity. Exhaustion is absorbing, so callers may use this to
/// suppress establishment I/O and maintenance without reserving an identity.
pub(crate) fn carrier_path_instance_identity_is_available() -> bool {
    carrier_path_instance_identity_is_available_in(&NEXT_CARRIER_PATH_INSTANCE_ID)
}

#[cfg(test)]
pub(crate) fn next_carrier_path_instance_id() -> CarrierPathInstanceId {
    try_next_carrier_path_instance_id().expect("test process carrier identity space")
}

#[cfg(test)]
#[path = "tests_path.rs"]
mod tests;
