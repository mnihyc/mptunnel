//! Carrier-neutral product work classifications.
//!
//! `FlowLane` describes latency versus throughput demand. These types instead
//! describe what product work may do to ordered ownership and sender queues.

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CarrierWorkKind {
    OwnerData,
    RepairData,
    Probe,
    Control,
}

impl CarrierWorkKind {
    pub(crate) fn is_ordering_owner(self) -> bool {
        matches!(self, Self::OwnerData)
    }

    pub(crate) fn carries_product_offsets(self) -> bool {
        matches!(self, Self::OwnerData | Self::RepairData)
    }

    pub(crate) fn counts_against_sender_extra_budget(self) -> bool {
        matches!(self, Self::RepairData)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ReliableWorkClass {
    Control,
    Data,
    Repair,
}
