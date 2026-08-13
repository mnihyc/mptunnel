//! Host-facing lifecycle and aggregate accounting for embedded runtimes.
//!
//! This deliberately exposes no data-plane objects. Native mobile hosts get a
//! cloneable generation handle, real listener readiness, cooperative shutdown,
//! and monotonic Product-boundary counters.

use super::readiness::{
    RuntimeGenerationControl, RuntimeGenerationPhase, RuntimeGenerationReadinessError,
};
use super::telemetry::{RuntimeTelemetry, active_flow_detail_capacity};
use crate::config::AppConfig;
use serde::Serialize;
use std::fmt;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct RuntimeHostControl {
    generation: RuntimeGenerationControl,
    telemetry: RuntimeTelemetry,
}

impl RuntimeHostControl {
    /// Creates a control whose accounting capacity matches `config`.
    pub fn for_config(config: &AppConfig) -> Self {
        Self::new(active_flow_detail_capacity(config.resources.max_streams))
    }

    fn new(active_flow_capacity: usize) -> Self {
        Self {
            generation: RuntimeGenerationControl::new(),
            telemetry: RuntimeTelemetry::generation_owner(active_flow_capacity),
        }
    }

    pub fn phase(&self) -> RuntimeHostPhase {
        self.generation.status().phase.into()
    }

    pub fn is_ready(&self) -> bool {
        self.generation.is_ready()
    }

    pub fn failure(&self) -> Option<Arc<str>> {
        self.generation.status().failure
    }

    pub async fn wait_until_ready(&self) -> Result<(), RuntimeHostReadinessError> {
        self.generation
            .wait_until_ready()
            .await
            .map_err(RuntimeHostReadinessError)
    }

    pub fn request_shutdown(&self) {
        self.generation.request_shutdown();
    }

    pub fn stats(&self) -> RuntimeHostStats {
        let snapshot = self.telemetry.snapshot();
        RuntimeHostStats {
            io: snapshot.io.into(),
            reliable: RuntimeHostFlowStats {
                io: snapshot.reliable.io.into(),
                opened: snapshot.reliable.flows.opened,
                active: snapshot.reliable.flows.active,
                completed: snapshot.reliable.flows.completed,
                failed: snapshot.reliable.flows.failed,
            },
            datagram: RuntimeHostFlowStats {
                io: snapshot.datagram.io.into(),
                opened: snapshot.datagram.flows.opened,
                active: snapshot.datagram.flows.active,
                completed: snapshot.datagram.flows.completed,
                failed: snapshot.datagram.flows.failed,
            },
            active_flow_capacity: snapshot.active_flow_capacity,
            active_flow_record_overflow: snapshot.active_flow_record_overflow,
            active_flow_record_overflow_total: snapshot.active_flow_record_overflow_total,
        }
    }

    pub(super) fn generation(&self) -> RuntimeGenerationControl {
        self.generation.clone()
    }

    pub(super) fn telemetry(&self) -> RuntimeTelemetry {
        self.telemetry.clone()
    }

    pub(super) fn active_flow_capacity(&self) -> usize {
        self.telemetry.snapshot().active_flow_capacity
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum RuntimeHostPhase {
    Starting,
    Ready,
    Stopping,
    Failed,
}

impl RuntimeHostPhase {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Starting => "starting",
            Self::Ready => "ready",
            Self::Stopping => "stopping",
            Self::Failed => "failed",
        }
    }
}

impl From<RuntimeGenerationPhase> for RuntimeHostPhase {
    fn from(value: RuntimeGenerationPhase) -> Self {
        match value {
            RuntimeGenerationPhase::Starting => Self::Starting,
            RuntimeGenerationPhase::Ready => Self::Ready,
            RuntimeGenerationPhase::Stopping => Self::Stopping,
            RuntimeGenerationPhase::Failed => Self::Failed,
        }
    }
}

#[derive(Debug)]
pub struct RuntimeHostReadinessError(RuntimeGenerationReadinessError);

impl fmt::Display for RuntimeHostReadinessError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

impl std::error::Error for RuntimeHostReadinessError {}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize)]
pub struct RuntimeHostIoStats {
    pub to_peer_bytes: u64,
    pub to_peer_packets: u64,
    pub from_peer_bytes: u64,
    pub from_peer_packets: u64,
}

impl From<super::telemetry::ProductIoSnapshot> for RuntimeHostIoStats {
    fn from(value: super::telemetry::ProductIoSnapshot) -> Self {
        Self {
            to_peer_bytes: value.to_peer_bytes,
            to_peer_packets: value.to_peer_packets,
            from_peer_bytes: value.from_peer_bytes,
            from_peer_packets: value.from_peer_packets,
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize)]
pub struct RuntimeHostFlowStats {
    pub io: RuntimeHostIoStats,
    pub opened: u64,
    pub active: u64,
    pub completed: u64,
    pub failed: u64,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize)]
pub struct RuntimeHostStats {
    pub io: RuntimeHostIoStats,
    pub reliable: RuntimeHostFlowStats,
    pub datagram: RuntimeHostFlowStats,
    pub active_flow_capacity: usize,
    pub active_flow_record_overflow: u64,
    pub active_flow_record_overflow_total: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn host_stats_start_at_zero_and_capacity_is_bounded() {
        let control = RuntimeHostControl::new(37);
        assert_eq!(control.phase(), RuntimeHostPhase::Starting);
        assert!(!control.is_ready());
        assert_eq!(control.stats().io, RuntimeHostIoStats::default());
        assert_eq!(control.stats().active_flow_capacity, 37);
    }

    #[tokio::test]
    async fn shutdown_interrupts_a_readiness_wait() {
        let control = RuntimeHostControl::new(1);
        control.request_shutdown();
        assert_eq!(control.phase(), RuntimeHostPhase::Stopping);
        assert!(control.wait_until_ready().await.is_err());
    }
}
