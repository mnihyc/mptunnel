//! Stable carrier identities shared by models and runtime services.
//!
//! A path service owns carrier lifetime and I/O; these values only identify
//! the carrier in model snapshots, intents, and exact-flight accounting.

use crate::protocol::{PathId, UnderlayProtocol};

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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct CarrierPathKey {
    pub(crate) underlay: UnderlayProtocol,
    pub(crate) path_id: PathId,
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
