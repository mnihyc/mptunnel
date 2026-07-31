//! Typed work accepted by reliable carrier actors.
//!
//! Commands describe carrier work and lifetime fences; queue capacity,
//! prioritization, and byte accounting remain in `queue`.

use super::ports::OpenedReliableCarrierStream;
use super::queue::TcpCapacityProbeLease;
use super::tcp::capacity::RequestTcpCapacityProbeLease;
use super::tcp::client::ClientTcpDatagramInbound;
use crate::model::path::{CarrierPathInstanceId, RelayPathInstance};
use crate::protocol::{DatagramFlowId, Frame, PathId, ResetReason, StreamId, TargetAddr};
use crate::runtime::error::RuntimeError;
use crate::scheduler::TrafficClass;
use std::sync::{
    Arc,
    atomic::{AtomicU8, Ordering},
};
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, oneshot};

pub(in crate::runtime) use super::queue::recv_reliable_path_command_during_drain;
pub(in crate::runtime) use super::queue::{
    ReliablePathCommandQueueSnapshot, ReliablePathCommandReceivers, recv_reliable_path_command,
    reliable_path_command_channels, reliable_path_command_pending_bytes,
    reliable_path_command_queue, reliable_path_command_writer_run_budget_bytes,
    reliable_path_command_writer_run_budget_items, reliable_path_command_writer_run_bytes,
    reliable_path_effective_frame_lane, reliable_path_frame_requires_capacity_command,
    reliable_path_receivers_closed, reliable_path_writer_frame_queue, reliable_stream_frame_queue,
    reliable_stream_frame_queue_for_payload, try_coalesce_reliable_path_writer_run,
    try_recv_reliable_path_command, try_recv_reliable_path_priority_command,
};
pub(in crate::runtime) use super::queue::{
    ReliablePathCommandSender, ReliablePathFrameReservation, ReliablePathLoadRegistration,
};
#[cfg(test)]
pub(in crate::runtime) use super::queue::{
    reliable_path_command_queue_for_payload, reliable_path_priority_headroom_frames,
    reliable_path_writer_frame_queue_for_payload,
    reliable_path_writer_should_coalesce_partial_bulk_run,
};

#[derive(Debug)]
struct CapacityProbeCommandTicketState {
    resolution: AtomicU8,
    resolved: tokio::sync::Notify,
}

/// One-shot publication or cancellation authority for queued capacity work.
/// Protocol geometry stays on the enclosing TCP or QUIC command/lease.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::runtime) enum CapacityProbeCommandResolution {
    Current = 0,
    Cancelled = 1,
    Published = 2,
}

#[derive(Debug, Clone)]
pub(in crate::runtime) struct CapacityProbeCommandTicket {
    state: Arc<CapacityProbeCommandTicketState>,
}

impl CapacityProbeCommandTicket {
    pub(in crate::runtime) fn new() -> Self {
        Self {
            state: Arc::new(CapacityProbeCommandTicketState {
                resolution: AtomicU8::new(CapacityProbeCommandResolution::Current as u8),
                resolved: tokio::sync::Notify::new(),
            }),
        }
    }

    pub(in crate::runtime) fn resolution(&self) -> CapacityProbeCommandResolution {
        match self.state.resolution.load(Ordering::Acquire) {
            value if value == CapacityProbeCommandResolution::Current as u8 => {
                CapacityProbeCommandResolution::Current
            }
            value if value == CapacityProbeCommandResolution::Cancelled as u8 => {
                CapacityProbeCommandResolution::Cancelled
            }
            value if value == CapacityProbeCommandResolution::Published as u8 => {
                CapacityProbeCommandResolution::Published
            }
            _ => unreachable!("invalid capacity command ticket resolution"),
        }
    }

    pub(in crate::runtime) fn is_current(&self) -> bool {
        self.resolution() == CapacityProbeCommandResolution::Current
    }

    fn resolve(&self, resolution: CapacityProbeCommandResolution) -> bool {
        debug_assert_ne!(resolution, CapacityProbeCommandResolution::Current);
        let resolved = self
            .state
            .resolution
            .compare_exchange(
                CapacityProbeCommandResolution::Current as u8,
                resolution as u8,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_ok();
        if resolved {
            self.state.resolved.notify_waiters();
        }
        resolved
    }

    pub(in crate::runtime) fn cancel(&self) -> bool {
        self.resolve(CapacityProbeCommandResolution::Cancelled)
    }

    pub(in crate::runtime) fn publish(&self) -> bool {
        self.resolve(CapacityProbeCommandResolution::Published)
    }

    pub(in crate::runtime) async fn resolved(&self) -> CapacityProbeCommandResolution {
        loop {
            let resolution = self.resolution();
            if resolution != CapacityProbeCommandResolution::Current {
                return resolution;
            }
            let notified = self.state.resolved.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            let resolution = self.resolution();
            if resolution != CapacityProbeCommandResolution::Current {
                return resolution;
            }
            notified.await;
        }
    }

    pub(in crate::runtime) async fn cancelled(&self) {
        if self.resolved().await == CapacityProbeCommandResolution::Cancelled {
            return;
        }
        std::future::pending::<()>().await;
    }
}

#[derive(Debug, Clone, Copy)]
pub(in crate::runtime) struct RequestTcpCapacityProbeRequest {
    pub(in crate::runtime) stream_id: StreamId,
    pub(in crate::runtime) path_instance: RelayPathInstance,
    pub(in crate::runtime) path_id: PathId,
    pub(in crate::runtime) measurement_id: u64,
    pub(in crate::runtime) train_payload_bytes: u64,
    pub(in crate::runtime) sample_floor_bytes: u64,
    pub(in crate::runtime) warmup_carrier_bytes: u64,
    pub(in crate::runtime) timing_slack_bytes: u64,
    pub(in crate::runtime) required_timed_carrier_bytes: u64,
    pub(in crate::runtime) baseline_expires_at: Instant,
    pub(in crate::runtime) write_expires_at: Instant,
    pub(in crate::runtime) expires_at: Instant,
}

#[derive(Debug)]
pub(in crate::runtime) struct TcpCapacityProbeCommand {
    pub(in crate::runtime) stream_id: StreamId,
    pub(in crate::runtime) path_instance: RelayPathInstance,
    pub(in crate::runtime) path_id: PathId,
    pub(in crate::runtime) measurement_id: u64,
    pub(in crate::runtime) train_payload_bytes: u64,
    pub(in crate::runtime) sample_floor_bytes: u64,
    pub(in crate::runtime) warmup_carrier_bytes: u64,
    pub(in crate::runtime) timing_slack_bytes: u64,
    pub(in crate::runtime) required_timed_carrier_bytes: u64,
    pub(in crate::runtime) baseline_expires_at: Instant,
    pub(in crate::runtime) write_expires_at: Instant,
    pub(in crate::runtime) expires_at: Instant,
    pub(in crate::runtime) request_lease: RequestTcpCapacityProbeLease,
    pub(in crate::runtime) _lease: TcpCapacityProbeLease,
}

impl TcpCapacityProbeCommand {
    pub(in crate::runtime) fn request_lease(&self) -> &RequestTcpCapacityProbeLease {
        &self.request_lease
    }

    pub(in crate::runtime) fn valid_request_tcp_train(&self) -> bool {
        let measurement_bytes = match self
            .timing_slack_bytes
            .checked_add(self.required_timed_carrier_bytes)
        {
            Some(bytes) => bytes,
            None => return false,
        };
        let train = self.warmup_carrier_bytes.checked_add(measurement_bytes);
        self.warmup_carrier_bytes > 0
            && self.timing_slack_bytes > 0
            && self.required_timed_carrier_bytes > 0
            && measurement_bytes >= self.sample_floor_bytes
            && train == Some(self.train_payload_bytes)
            && self.baseline_expires_at < self.write_expires_at
            && self.write_expires_at < self.expires_at
    }
}

impl Drop for TcpCapacityProbeCommand {
    fn drop(&mut self) {
        self.request_lease.cancel();
    }
}

pub(in crate::runtime) enum ReliablePathCommand {
    PrepareConnection {
        open_deadline: tokio::time::Instant,
        endpoint_generation: u64,
        response: oneshot::Sender<Result<Option<Duration>, RuntimeError>>,
    },
    OpenStream {
        stream_id: StreamId,
        attempt_id: ClientTcpOpenAttemptId,
        observed_carrier_instance: u64,
        target: TargetAddr,
        lane: TrafficClass,
        advertised_recv_max_offset: u64,
        open_deadlines: ClientTcpOpenDeadlines,
        session_commands: ReliablePathCommandSender,
        response: oneshot::Sender<ClientTcpOpenResponse>,
    },
    CancelTcpOpen {
        stream_id: StreamId,
        attempt_id: ClientTcpOpenAttemptId,
    },
    OpenDatagramAttachment {
        attachment_id: u64,
        frames: mpsc::Sender<Result<ClientTcpDatagramInbound, RuntimeError>>,
        failure: oneshot::Sender<()>,
        open_deadline: tokio::time::Instant,
        response: oneshot::Sender<Result<CarrierPathInstanceId, RuntimeError>>,
    },
    OpenDatagramFlow {
        attachment_id: u64,
        flow_id: DatagramFlowId,
        target: TargetAddr,
        open_deadline: tokio::time::Instant,
        response: oneshot::Sender<Result<(), RuntimeError>>,
    },
    SendDatagramFrame {
        attachment_id: u64,
        frame: Frame,
        write_deadline: tokio::time::Instant,
        expires_at: Option<tokio::time::Instant>,
        response: oneshot::Sender<Result<(), RuntimeError>>,
    },
    CloseDatagramAttachment {
        attachment_id: u64,
        response: Option<oneshot::Sender<Result<(), RuntimeError>>>,
    },
    SendFrame(Frame),
    SendTcpCapacityProbe(TcpCapacityProbeCommand),
    ResetAndCloseStream {
        stream_id: StreamId,
        reason: ResetReason,
    },
    CloseStream(StreamId),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::runtime) struct ClientTcpOpenAttemptId(pub(in crate::runtime) u64);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::runtime) struct ClientTcpOpenDeadlines {
    pub(in crate::runtime) live: tokio::time::Instant,
    pub(in crate::runtime) setup: tokio::time::Instant,
}

impl ClientTcpOpenDeadlines {
    pub(in crate::runtime) fn fixed(deadline: tokio::time::Instant) -> Self {
        Self {
            live: deadline,
            setup: deadline,
        }
    }

    pub(in crate::runtime) fn from_timeouts(
        now: tokio::time::Instant,
        live: Duration,
        setup: Duration,
    ) -> Self {
        Self {
            live: now + live,
            setup: now + setup.max(live),
        }
    }

    pub(in crate::runtime) fn for_carrier_instance(
        self,
        observed_instance: u64,
        current_instance: u64,
    ) -> tokio::time::Instant {
        if observed_instance != 0 && observed_instance == current_instance {
            self.live
        } else {
            self.setup
        }
    }
}

pub(in crate::runtime) struct ClientTcpOpenedStream {
    pub(in crate::runtime) carrier: OpenedReliableCarrierStream,
    pub(in crate::runtime) open_deadline: tokio::time::Instant,
}

// This value crosses one one-shot channel. Keeping the success payload inline
// avoids allocating every successful TCP stream open solely to equalize errors.
#[allow(clippy::large_enum_variant)]
pub(in crate::runtime) enum ClientTcpOpenResponse {
    Opened(ClientTcpOpenedStream),
    RejectedWithoutOpen(RuntimeError),
    FailedAfterOpen(RuntimeError),
}
