//! Stable carrier identities shared by models and runtime services.
//!
//! A path service owns carrier lifetime and I/O; these values only identify
//! the carrier in model snapshots, intents, and exact-flight accounting.

use crate::protocol::{PathId, UnderlayProtocol};
use std::time::Instant;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct RelayPathKey {
    pub(crate) underlay: UnderlayProtocol,
    pub(crate) index: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct RelayPathInstance {
    pub(crate) key: RelayPathKey,
    pub(crate) id: u64,
}

/// Attachment-set placement of one carrier instance.
///
/// Placement is transport-neutral: TCP and QUIC retain separate carrier
/// mechanics while request/response policy reasons over the same lifecycle
/// state. `Active` does not by itself grant ordered product ownership.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RelayPathPlacement {
    Active,
    Repair,
    Validation,
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

/// Opaque lifetime identity for one physical carrier attachment.
///
/// `CarrierPathKey` names a logical path; this value changes when that path is
/// replaced so evidence and in-flight ownership cannot cross attachment lives.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct CarrierPathInstanceId(u64);

impl CarrierPathInstanceId {
    pub(crate) fn from_raw(raw: u64) -> Self {
        Self(raw)
    }

    #[cfg(feature = "lab-diagnostics")]
    pub(crate) fn as_u64(self) -> u64 {
        self.0
    }
}

#[cfg(test)]
#[path = "path_test.rs"]
mod tests;
