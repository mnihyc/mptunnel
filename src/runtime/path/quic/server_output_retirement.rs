//! One output-retirement authority shared by a QUIC stream's writer and owner.

use crate::protocol::StreamId;
use crate::runtime::error::RuntimeError;
use crate::runtime::path::{ServerCarrierPathRegistration, ServerStreamPort};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

/// Withdraws the exact output once, including when its owner is cancelled
/// after the writer retires it but before the native finish await completes.
pub(super) struct ServerUdpOutputRetirement {
    streams: ServerStreamPort,
    path_registration: ServerCarrierPathRegistration,
    stream_id: StreamId,
    retired: AtomicBool,
}

impl ServerUdpOutputRetirement {
    pub(super) fn new(
        streams: ServerStreamPort,
        path_registration: ServerCarrierPathRegistration,
        stream_id: StreamId,
    ) -> Arc<Self> {
        Arc::new(Self {
            streams,
            path_registration,
            stream_id,
            retired: AtomicBool::new(false),
        })
    }

    pub(super) fn retire(&self) -> Result<(), RuntimeError> {
        if self.retired.swap(true, Ordering::AcqRel) {
            return Ok(());
        }
        // Registration carries the authenticated session and exact physical
        // path identity; the port validates its service ownership. Complete
        // synchronous scheduling withdrawal before any caller can await FIN.
        self.streams
            .detach_path(&self.path_registration, self.stream_id)
    }
}
