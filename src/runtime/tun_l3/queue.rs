//! Direction-local, byte-bounded admission for complete IP packets.
//!
//! A permit follows one packet until its final local carrier or packet-device
//! queue accepts it. Attachment count therefore cannot multiply the configured
//! packet envelope, and a newer packet cannot displace an older accepted one.

use crate::runtime::error::RuntimeError;
use std::sync::Arc;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

// IPv4 permits a 576-byte path MTU. Charging smaller packets at that floor
// bounds queue-record overhead without changing the configured byte envelope.
const MINIMUM_IP_PACKET_CHARGE: usize = 576;

#[derive(Debug, Clone)]
pub(in crate::runtime) struct IpPacketQueueBudget {
    permits: Arc<Semaphore>,
    minimum_charge: usize,
}

pub(in crate::runtime) struct IpPacketQueuePermit {
    _permit: OwnedSemaphorePermit,
}

impl std::fmt::Debug for IpPacketQueuePermit {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("IpPacketQueuePermit")
            .finish_non_exhaustive()
    }
}

impl IpPacketQueueBudget {
    pub(in crate::runtime) fn new(max_bytes: usize) -> Self {
        let max_bytes = max_bytes.clamp(1, Semaphore::MAX_PERMITS);
        Self {
            permits: Arc::new(Semaphore::new(max_bytes)),
            minimum_charge: MINIMUM_IP_PACKET_CHARGE.min(max_bytes),
        }
    }

    pub(in crate::runtime) fn try_reserve(
        &self,
        packet_bytes: usize,
    ) -> Result<IpPacketQueuePermit, RuntimeError> {
        let charge = packet_bytes.max(self.minimum_charge);
        let permits = u32::try_from(charge).map_err(|_| RuntimeError::SenderServiceBlocked)?;
        let permit = self
            .permits
            .clone()
            .try_acquire_many_owned(permits)
            .map_err(|_| RuntimeError::SenderServiceBlocked)?;
        Ok(IpPacketQueuePermit { _permit: permit })
    }

    #[cfg(test)]
    pub(in crate::runtime) fn available_bytes(&self) -> usize {
        self.permits.available_permits()
    }
}

#[cfg(test)]
#[path = "tests_queue.rs"]
mod tests;
