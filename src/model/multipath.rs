//! Connection-level accounting shared by multipath scheduling directions.
//!
//! TCP and QUIC retain independent congestion control and recovery. This model
//! only bounds optional duplicate traffic relative to acknowledged progress of
//! original connection data.

use crate::performance::MppPerformanceConfig;

#[derive(Debug, Clone, Copy)]
pub(crate) struct ExtraTrafficBudget {
    delivered_data_bytes: u64,
    reinjected_bytes: u64,
    startup_floor_bytes: u64,
    percent_budget: u16,
}

impl ExtraTrafficBudget {
    pub(crate) fn new(
        delivered_data_bytes: u64,
        reinjected_bytes: u64,
        startup_floor_bytes: usize,
        performance: MppPerformanceConfig,
    ) -> Self {
        Self {
            delivered_data_bytes,
            reinjected_bytes,
            startup_floor_bytes: startup_floor_bytes as u64,
            percent_budget: performance.extra_traffic_hint_percent,
        }
    }

    pub(crate) fn limit_bytes(self) -> u64 {
        self.startup_floor_bytes.saturating_add(
            self.delivered_data_bytes
                .saturating_mul(self.percent_budget as u64)
                / 100,
        )
    }

    pub(crate) fn remaining_bytes(self) -> usize {
        self.limit_bytes()
            .saturating_sub(self.reinjected_bytes)
            .min(usize::MAX as u64) as usize
    }

    pub(crate) fn can_spend(self, bytes: usize) -> bool {
        self.reinjected_bytes.saturating_add(bytes as u64) <= self.limit_bytes()
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct ExtraTrafficLedger {
    delivered_data_bytes: u64,
    reinjected_bytes: u64,
}

impl ExtraTrafficLedger {
    #[cfg(test)]
    pub(crate) fn delivered_data_bytes(self) -> u64 {
        self.delivered_data_bytes
    }

    pub(crate) fn reinjected_bytes(self) -> u64 {
        self.reinjected_bytes
    }

    pub(crate) fn record_delivered_data(&mut self, bytes: usize) {
        self.delivered_data_bytes = self.delivered_data_bytes.saturating_add(bytes as u64);
    }

    pub(crate) fn record_reinjection(&mut self, bytes: usize) {
        self.reinjected_bytes = self.reinjected_bytes.saturating_add(bytes as u64);
    }

    pub(crate) fn budget(
        self,
        startup_floor_bytes: usize,
        performance: MppPerformanceConfig,
    ) -> ExtraTrafficBudget {
        ExtraTrafficBudget::new(
            self.delivered_data_bytes,
            self.reinjected_bytes(),
            startup_floor_bytes,
            performance,
        )
    }
}

#[cfg(test)]
#[path = "tests_multipath.rs"]
mod tests;
