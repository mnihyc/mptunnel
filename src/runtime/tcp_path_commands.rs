use super::tcp_path::TcpPathStream;
use super::*;
use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Clone)]
pub(super) struct TcpPathSessionCommandSender {
    control: mpsc::Sender<TcpPathSessionCommand>,
    priority: mpsc::Sender<TcpPathSessionCommand>,
    data: mpsc::Sender<TcpPathSessionCommand>,
    metrics: Arc<TcpPathSessionCommandQueueMetrics>,
}

pub(super) struct TcpPathSessionCommandReceivers {
    control: mpsc::Receiver<TcpPathSessionCommand>,
    priority: mpsc::Receiver<TcpPathSessionCommand>,
    data: mpsc::Receiver<TcpPathSessionCommand>,
    fair_data: TcpPathFairDataQueue,
    metrics: Arc<TcpPathSessionCommandQueueMetrics>,
}

#[derive(Default)]
struct TcpPathFairDataQueue {
    flows: VecDeque<TcpPathDataFlowQueue>,
}

struct TcpPathDataFlowQueue {
    stream_id: StreamId,
    commands: VecDeque<TcpPathSessionCommand>,
}

impl TcpPathFairDataQueue {
    fn push(&mut self, command: TcpPathSessionCommand) {
        let stream_id = tcp_path_command_stream_id(&command);
        if let Some(flow) = self
            .flows
            .iter_mut()
            .find(|flow| flow.stream_id == stream_id)
        {
            flow.commands.push_back(command);
            return;
        }
        let mut commands = VecDeque::new();
        commands.push_back(command);
        self.flows.push_back(TcpPathDataFlowQueue {
            stream_id,
            commands,
        });
    }

    fn pop_round_robin(&mut self) -> Option<TcpPathSessionCommand> {
        let flow_count = self.flows.len();
        for _ in 0..flow_count {
            let mut flow = self.flows.pop_front()?;
            let command = flow.commands.pop_front();
            if !flow.commands.is_empty() {
                self.flows.push_back(flow);
            }
            if command.is_some() {
                return command;
            }
        }
        None
    }

    fn is_empty(&self) -> bool {
        self.flows.iter().all(|flow| flow.commands.is_empty())
    }
}

#[derive(Default)]
struct TcpPathSessionCommandQueueMetrics {
    pending_bytes: AtomicU64,
}

impl TcpPathSessionCommandQueueMetrics {
    fn add_pending_bytes(&self, bytes: usize) {
        self.pending_bytes
            .fetch_add(bytes as u64, Ordering::Relaxed);
    }

    fn release_pending_bytes(&self, bytes: usize) {
        let bytes = bytes as u64;
        let _ = self
            .pending_bytes
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                Some(current.saturating_sub(bytes))
            });
    }

    #[cfg_attr(not(any(test, feature = "lab-diagnostics")), allow(dead_code))]
    fn pending_bytes(&self) -> u64 {
        self.pending_bytes.load(Ordering::Relaxed)
    }
}

impl TcpPathSessionCommandSender {
    pub(super) async fn send_control(
        &self,
        command: TcpPathSessionCommand,
    ) -> Result<(), mpsc::error::SendError<TcpPathSessionCommand>> {
        #[cfg(feature = "lab-diagnostics")]
        let started = Instant::now();
        let result = self.control.send(command).await;
        #[cfg(feature = "lab-diagnostics")]
        lab_perf_record("runtime.path_queue.control_send", started.elapsed(), 0);
        result
    }

    pub(super) async fn send_frame(
        &self,
        frame: Frame,
        lane: FlowLane,
    ) -> Result<(), RuntimeError> {
        let bytes = frame_pacing_bytes(&frame);
        let effective_lane = tcp_path_effective_frame_lane(&frame, lane);
        let queue = if tcp_path_frame_uses_priority_queue(effective_lane) {
            &self.priority
        } else {
            &self.data
        };
        #[cfg(feature = "lab-diagnostics")]
        let started = Instant::now();
        let result = match queue.reserve().await {
            Ok(permit) => {
                self.metrics.add_pending_bytes(bytes);
                permit.send(TcpPathSessionCommand::SendFrame(frame));
                Ok(())
            }
            Err(_) => Err(RuntimeError::TcpPathSessionClosed),
        };
        #[cfg(feature = "lab-diagnostics")]
        lab_perf_record(
            if tcp_path_frame_uses_priority_queue(effective_lane) {
                "runtime.path_queue.priority_send"
            } else {
                "runtime.path_queue.data_send"
            },
            started.elapsed(),
            bytes,
        );
        result
    }

    pub(super) fn try_send_frame(
        &self,
        frame: Frame,
        lane: FlowLane,
    ) -> Result<bool, RuntimeError> {
        let bytes = frame_pacing_bytes(&frame);
        let effective_lane = tcp_path_effective_frame_lane(&frame, lane);
        let queue = if tcp_path_frame_uses_priority_queue(effective_lane) {
            &self.priority
        } else {
            &self.data
        };
        match queue.try_reserve() {
            Ok(permit) => {
                self.metrics.add_pending_bytes(bytes);
                permit.send(TcpPathSessionCommand::SendFrame(frame));
                Ok(true)
            }
            Err(mpsc::error::TrySendError::Full(_)) => Ok(false),
            Err(mpsc::error::TrySendError::Closed(_)) => Err(RuntimeError::TcpPathSessionClosed),
        }
    }

    #[cfg_attr(not(any(test, feature = "lab-diagnostics")), allow(dead_code))]
    pub(super) fn pending_bytes(&self) -> u64 {
        self.metrics.pending_bytes()
    }

    pub(super) fn is_closed(&self) -> bool {
        self.control.is_closed() && self.priority.is_closed() && self.data.is_closed()
    }

    pub(super) fn same_channel(&self, other: &Self) -> bool {
        self.control.same_channel(&other.control)
            && self.priority.same_channel(&other.priority)
            && self.data.same_channel(&other.data)
    }
}

pub(super) fn tcp_path_frame_uses_priority_queue(lane: FlowLane) -> bool {
    matches!(
        lane,
        FlowLane::Control | FlowLane::Latency | FlowLane::RealtimeDatagram
    )
}

pub(super) fn tcp_path_effective_frame_lane(frame: &Frame, stream_lane: FlowLane) -> FlowLane {
    match frame {
        Frame::StreamData { .. } => stream_lane,
        Frame::DatagramData { .. } | Frame::DatagramFeedback { .. } => FlowLane::RealtimeDatagram,
        _ => FlowLane::Control,
    }
}

pub(super) fn tcp_path_session_command_channels(
    queue: usize,
) -> (TcpPathSessionCommandSender, TcpPathSessionCommandReceivers) {
    let queue = queue.max(1);
    let (control_tx, control_rx) = mpsc::channel(queue);
    let (priority_tx, priority_rx) = mpsc::channel(queue);
    let (data_tx, data_rx) = mpsc::channel(queue);
    let metrics = Arc::new(TcpPathSessionCommandQueueMetrics::default());
    (
        TcpPathSessionCommandSender {
            control: control_tx,
            priority: priority_tx,
            data: data_tx,
            metrics: metrics.clone(),
        },
        TcpPathSessionCommandReceivers {
            control: control_rx,
            priority: priority_rx,
            data: data_rx,
            fair_data: TcpPathFairDataQueue::default(),
            metrics,
        },
    )
}

fn tcp_receiver_may_recv<T>(receiver: &mpsc::Receiver<T>) -> bool {
    !receiver.is_closed() || !receiver.is_empty()
}

pub(super) fn tcp_path_receivers_closed(receivers: &TcpPathSessionCommandReceivers) -> bool {
    !tcp_receiver_may_recv(&receivers.control)
        && !tcp_receiver_may_recv(&receivers.priority)
        && !tcp_receiver_may_recv(&receivers.data)
        && receivers.fair_data.is_empty()
}

pub(super) async fn recv_tcp_path_command(
    receivers: &mut TcpPathSessionCommandReceivers,
) -> Option<TcpPathSessionCommand> {
    if let Some(command) = recv_ready_priority_command(receivers) {
        release_tcp_path_command_pending_bytes(receivers, &command);
        return Some(command);
    }
    drain_ready_data_commands(receivers);
    if let Some(command) = receivers.fair_data.pop_round_robin() {
        release_tcp_path_command_pending_bytes(receivers, &command);
        return Some(command);
    }
    let control_may_recv = tcp_receiver_may_recv(&receivers.control);
    let priority_may_recv = tcp_receiver_may_recv(&receivers.priority);
    let data_may_recv = tcp_receiver_may_recv(&receivers.data);
    let command = match (control_may_recv, priority_may_recv, data_may_recv) {
        (true, true, true) => {
            tokio::select! {
                biased;
                command = receivers.control.recv() => TcpPathReceivedCommand::Priority(command),
                command = receivers.priority.recv() => TcpPathReceivedCommand::Priority(command),
                command = receivers.data.recv() => TcpPathReceivedCommand::Data(command),
            }
        }
        (true, true, false) => {
            tokio::select! {
                biased;
                command = receivers.control.recv() => TcpPathReceivedCommand::Priority(command),
                command = receivers.priority.recv() => TcpPathReceivedCommand::Priority(command),
            }
        }
        (true, false, true) => {
            tokio::select! {
                biased;
                command = receivers.control.recv() => TcpPathReceivedCommand::Priority(command),
                command = receivers.data.recv() => TcpPathReceivedCommand::Data(command),
            }
        }
        (false, true, true) => {
            tokio::select! {
                biased;
                command = receivers.priority.recv() => TcpPathReceivedCommand::Priority(command),
                command = receivers.data.recv() => TcpPathReceivedCommand::Data(command),
            }
        }
        (true, false, false) => TcpPathReceivedCommand::Priority(receivers.control.recv().await),
        (false, true, false) => TcpPathReceivedCommand::Priority(receivers.priority.recv().await),
        (false, false, true) => TcpPathReceivedCommand::Data(receivers.data.recv().await),
        (false, false, false) => TcpPathReceivedCommand::Priority(None),
    };
    match command {
        TcpPathReceivedCommand::Priority(command) => {
            if let Some(command) = &command {
                release_tcp_path_command_pending_bytes(receivers, command);
            }
            command
        }
        TcpPathReceivedCommand::Data(Some(command)) => {
            receivers.fair_data.push(command);
            drain_ready_data_commands(receivers);
            let command = receivers.fair_data.pop_round_robin();
            if let Some(command) = &command {
                release_tcp_path_command_pending_bytes(receivers, command);
            }
            command
        }
        TcpPathReceivedCommand::Data(None) => {
            let command = receivers.fair_data.pop_round_robin();
            if let Some(command) = &command {
                release_tcp_path_command_pending_bytes(receivers, command);
            }
            command
        }
    }
}

enum TcpPathReceivedCommand {
    Priority(Option<TcpPathSessionCommand>),
    Data(Option<TcpPathSessionCommand>),
}

fn recv_ready_priority_command(
    receivers: &mut TcpPathSessionCommandReceivers,
) -> Option<TcpPathSessionCommand> {
    if let Ok(command) = receivers.control.try_recv() {
        return Some(command);
    }
    receivers.priority.try_recv().ok()
}

fn drain_ready_data_commands(receivers: &mut TcpPathSessionCommandReceivers) {
    while let Ok(command) = receivers.data.try_recv() {
        receivers.fair_data.push(command);
    }
}

fn release_tcp_path_command_pending_bytes(
    receivers: &TcpPathSessionCommandReceivers,
    command: &TcpPathSessionCommand,
) {
    receivers
        .metrics
        .release_pending_bytes(tcp_path_command_pacing_bytes(command));
}

fn tcp_path_command_pacing_bytes(command: &TcpPathSessionCommand) -> usize {
    match command {
        TcpPathSessionCommand::SendFrame(frame) => frame_pacing_bytes(frame),
        TcpPathSessionCommand::OpenStream { .. } | TcpPathSessionCommand::CloseStream(_) => 0,
    }
}

fn tcp_path_command_stream_id(command: &TcpPathSessionCommand) -> StreamId {
    match command {
        TcpPathSessionCommand::SendFrame(frame) => tcp_path_frame_stream_id(frame),
        TcpPathSessionCommand::OpenStream { stream_id, .. }
        | TcpPathSessionCommand::CloseStream(stream_id) => *stream_id,
    }
}

fn tcp_path_frame_stream_id(frame: &Frame) -> StreamId {
    match frame {
        Frame::OpenStream { stream_id, .. }
        | Frame::StreamData { stream_id, .. }
        | Frame::StreamAck { stream_id, .. }
        | Frame::StreamMaxData { stream_id, .. }
        | Frame::StreamFin { stream_id, .. }
        | Frame::StreamDetach { stream_id }
        | Frame::StreamReset { stream_id, .. } => *stream_id,
        _ => StreamId(0),
    }
}

pub(super) enum TcpPathSessionCommand {
    OpenStream {
        stream_id: StreamId,
        target: TargetAddr,
        ingress: IngressKind,
        lane: FlowLane,
        role: StreamOpenRole,
        session_commands: TcpPathSessionCommandSender,
        response: oneshot::Sender<Result<TcpPathStream, RuntimeError>>,
    },
    SendFrame(Frame),
    CloseStream(StreamId),
}
