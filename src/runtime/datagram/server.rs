//! Session-level UDP target flows shared by TCP and QUIC carrier attachments.

use crate::model::datagram::{DatagramAdmission, DatagramPayloadIdentity, DatagramReceiveWindow};
use crate::mux::MuxLimits;
use crate::outbound;
use crate::product::InboundId;
use crate::protocol::{DatagramFlowId, DatagramId, Frame, OffsetRange, SessionId, TargetAddr};
use crate::runtime::error::RuntimeError;
use crate::runtime::path::commands::ReliablePathCommandSender;
use crate::runtime::path::{
    AcceptedServerDatagramFlow, ServerDatagramOpenError, ServerDatagramOpenRequest,
    ServerDatagramPort, ServerDatagramPortBackend, ServerDatagramSendOutcome,
    ServerDatagramWorkerMessage, ServerStreamPort,
};
use crate::runtime::product_policy::{ClientIngressRouter, ClientPolicyDisposition, ClientRoute};
use crate::runtime::telemetry::{ProductFlowLease, RuntimeTelemetry};
use crate::scheduler::TrafficClass;
use bytes::Bytes;
use std::collections::{HashMap, VecDeque};
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;
use tokio::sync::{OnceCell, mpsc};
use tokio::time::Instant;

#[cfg(feature = "lab-diagnostics")]
use crate::lab_diagnostics::lab_diagnostic;

const OUTBOUND_UDP_RECV_BUFFER_BYTES: usize = u16::MAX as usize;
const MAX_DATAGRAM_RESPONSE_ROUTES: usize = 2;

type ServerDatagramFlowKey = (SessionId, DatagramFlowId);
type ServerDatagramFlowRegistry =
    Arc<Mutex<HashMap<ServerDatagramFlowKey, Arc<ServerDatagramFlowSlot>>>>;

/// Composition-owned UDP target policy and session-level flow registry.
pub(in crate::runtime) struct ServerDatagramService {
    router: ClientIngressRouter,
    inbound: InboundId,
    session_retention_timeout: Duration,
    mux_limits: MuxLimits,
    reliable_streams: ServerStreamPort,
    telemetry: RuntimeTelemetry,
    flows: ServerDatagramFlowRegistry,
}

struct ServerDatagramFlowSlot {
    target: TargetAddr,
    worker: OnceCell<mpsc::Sender<ServerDatagramWorkerMessage>>,
    attachments: Arc<AtomicUsize>,
    attachment_changes: Arc<tokio::sync::Notify>,
}

struct ServerDatagramAttachment {
    count: Arc<AtomicUsize>,
    changes: Arc<tokio::sync::Notify>,
}

impl ServerDatagramAttachment {
    fn new(count: Arc<AtomicUsize>, changes: Arc<tokio::sync::Notify>) -> Self {
        count.fetch_add(1, Ordering::AcqRel);
        changes.notify_one();
        Self { count, changes }
    }
}

impl Drop for ServerDatagramAttachment {
    fn drop(&mut self) {
        let previous = self.count.fetch_sub(1, Ordering::AcqRel);
        debug_assert!(previous > 0, "datagram attachment count underflow");
        self.changes.notify_one();
    }
}

pub(in crate::runtime) struct ServerDatagramServiceConfig {
    pub(in crate::runtime) router: ClientIngressRouter,
    pub(in crate::runtime) inbound: InboundId,
    pub(in crate::runtime) session_retention_timeout: Duration,
    pub(in crate::runtime) mux_limits: MuxLimits,
    pub(in crate::runtime) reliable_streams: ServerStreamPort,
    pub(in crate::runtime) telemetry: RuntimeTelemetry,
}

impl ServerDatagramService {
    pub(in crate::runtime) fn path_port(config: ServerDatagramServiceConfig) -> ServerDatagramPort {
        let ServerDatagramServiceConfig {
            router,
            inbound,
            session_retention_timeout,
            mux_limits,
            reliable_streams,
            telemetry,
        } = config;
        ServerDatagramPort::new(Arc::new(Self {
            router,
            inbound,
            session_retention_timeout,
            mux_limits,
            reliable_streams,
            telemetry,
            flows: Arc::new(Mutex::new(HashMap::new())),
        }))
    }

    fn flow_slot(
        &self,
        key: ServerDatagramFlowKey,
        target: TargetAddr,
    ) -> Result<Arc<ServerDatagramFlowSlot>, ServerDatagramOpenError> {
        let mut flows = self.flows.lock().expect("server datagram registry lock");
        if let Some(slot) = flows.get(&key) {
            if slot.target != target {
                return Err(ServerDatagramOpenError::new(RuntimeError::Protocol(
                    "datagram flow reopened with a different target",
                )));
            }
            return Ok(slot.clone());
        }
        let session_flows = flows
            .keys()
            .filter(|(session_id, _)| *session_id == key.0)
            .count();
        if flows.len() >= self.mux_limits.max_streams
            || session_flows >= self.mux_limits.max_streams
        {
            return Err(ServerDatagramOpenError::capacity());
        }
        let slot = Arc::new(ServerDatagramFlowSlot {
            target,
            worker: OnceCell::new(),
            attachments: Arc::new(AtomicUsize::new(0)),
            attachment_changes: Arc::new(tokio::sync::Notify::new()),
        });
        flows.insert(key, slot.clone());
        Ok(slot)
    }

    fn remove_flow_slot_if_current(
        registry: &ServerDatagramFlowRegistry,
        key: ServerDatagramFlowKey,
        slot: &Arc<ServerDatagramFlowSlot>,
    ) {
        let mut flows = registry.lock().expect("server datagram registry lock");
        if flows
            .get(&key)
            .is_some_and(|current| Arc::ptr_eq(current, slot))
        {
            flows.remove(&key);
        }
    }
}

impl ServerDatagramPortBackend for ServerDatagramService {
    fn open<'a>(
        &'a self,
        request: ServerDatagramOpenRequest,
    ) -> Pin<
        Box<
            dyn Future<Output = Result<AcceptedServerDatagramFlow, ServerDatagramOpenError>>
                + Send
                + 'a,
        >,
    > {
        Box::pin(async move {
            let ServerDatagramOpenRequest {
                session_id,
                principal_permit,
                flow_id,
                target,
                commands,
            } = request;
            outbound::validate_target(&target)
                .map_err(|error| ServerDatagramOpenError::new(error.into()))?;
            let key = (session_id, flow_id);
            let slot = self.flow_slot(key, target.clone())?;
            let principal = principal_permit.principal().clone();
            let worker = slot
                .worker
                .get_or_try_init(|| async {
                    let route =
                        self.router
                            .route_mpp_udp(&target, principal, self.inbound.clone())?;
                    let plan = match route {
                        ClientRoute::Open(plan) => plan,
                        ClientRoute::Deny(ClientPolicyDisposition::Reject) => {
                            return Err(RuntimeError::RouteRejected);
                        }
                        ClientRoute::Deny(ClientPolicyDisposition::Drop) => {
                            return Err(RuntimeError::RouteDropped);
                        }
                    };
                    let opened = plan.open_udp(&target).await?;
                    let crate::runtime::outbound_registry::OpenedUdpOutbound::Local {
                        socket: outbound_socket,
                        _gateway_lease,
                        _product_flow,
                    } = opened
                    else {
                        return Err(RuntimeError::Protocol(
                            "MPP inbound cannot route UDP to an MPP outbound",
                        ));
                    };
                    let telemetry_flow = self
                        .telemetry
                        .scoped(_product_flow.scope().clone())
                        .open_datagram_flow(Some(session_id), flow_id, target.clone());
                    let realtime_registration =
                        self.reliable_streams.register_realtime_flow(session_id);
                    Ok::<_, RuntimeError>(spawn_server_datagram_flow_worker(
                        key,
                        outbound_socket,
                        _gateway_lease,
                        _product_flow,
                        self.mux_limits,
                        self.session_retention_timeout,
                        telemetry_flow,
                        realtime_registration,
                        self.flows.clone(),
                        Arc::downgrade(&slot),
                    ))
                })
                .await;
            let worker = match worker {
                Ok(worker) => worker.clone(),
                Err(error) => {
                    Self::remove_flow_slot_if_current(&self.flows, key, &slot);
                    return Err(ServerDatagramOpenError::new(error));
                }
            };
            let attachment = ServerDatagramAttachment::new(
                slot.attachments.clone(),
                slot.attachment_changes.clone(),
            );
            let route_lifetime = Arc::new(());
            let (attached, attachment_ready) = tokio::sync::oneshot::channel();
            worker
                .send(ServerDatagramWorkerMessage::Attach {
                    commands: commands.clone(),
                    attachment: Arc::downgrade(&route_lifetime),
                    attached,
                })
                .await
                .map_err(|_| {
                    ServerDatagramOpenError::new(RuntimeError::Protocol(
                        "server datagram worker closed during attachment",
                    ))
                })?;
            attachment_ready.await.map_err(|_| {
                ServerDatagramOpenError::new(RuntimeError::Protocol(
                    "server datagram worker closed during attachment",
                ))
            })?;
            Ok(AcceptedServerDatagramFlow::holding(
                flow_id,
                worker,
                commands,
                route_lifetime,
                attachment,
            ))
        })
    }
}

fn server_datagram_request_queue_len(mux_limits: MuxLimits) -> usize {
    let unit = mux_limits.max_payload_bytes.max(1);
    mux_limits
        .max_datagram_queue_bytes
        .saturating_div(unit)
        .max(1)
}

struct CachedServerDatagram {
    datagram_id: DatagramId,
    deadline: Instant,
    response: Bytes,
    queued_once: bool,
}

struct ServerDatagramRoute {
    commands: ReliablePathCommandSender,
    attachment: Weak<()>,
    next_response_id: u64,
}

struct ServerDatagramFlowState {
    received_requests: DatagramReceiveWindow,
    next_response_id: u64,
    routes: VecDeque<ServerDatagramRoute>,
    cached_responses: VecDeque<CachedServerDatagram>,
    cached_response_bytes: usize,
}

impl ServerDatagramFlowState {
    fn new(mux_limits: MuxLimits) -> Self {
        Self {
            received_requests: DatagramReceiveWindow::new(mux_limits.max_ack_ranges),
            next_response_id: 0,
            routes: VecDeque::new(),
            cached_responses: VecDeque::new(),
            cached_response_bytes: 0,
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn spawn_server_datagram_flow_worker(
    key: ServerDatagramFlowKey,
    mut outbound_socket: outbound::OutboundUdpSocket,
    mut gateway_lease: Option<crate::runtime::gateway::GatewayFlowLease>,
    product_flow: crate::runtime::outbound_registry::OpenedProductFlow,
    mux_limits: MuxLimits,
    session_retention_timeout: Duration,
    telemetry_flow: ProductFlowLease,
    realtime_registration: crate::runtime::path::ServerRealtimeFlowLease,
    registry: ServerDatagramFlowRegistry,
    slot: Weak<ServerDatagramFlowSlot>,
) -> mpsc::Sender<ServerDatagramWorkerMessage> {
    let (requests_tx, mut requests_rx) =
        mpsc::channel(server_datagram_request_queue_len(mux_limits));
    tokio::spawn(async move {
        let _product_flow = product_flow;
        let product_counter = telemetry_flow.counter();
        let mut failure = None;
        let response_buffer_len = mux_limits
            .max_payload_bytes
            .min(OUTBOUND_UDP_RECV_BUFFER_BYTES);
        let mut response_buffer = bytes::BytesMut::zeroed(response_buffer_len);
        let mut state = ServerDatagramFlowState::new(mux_limits);
        let mut idle_deadline = Instant::now() + session_retention_timeout;
        loop {
            prune_server_datagram_state(&mut state, mux_limits, Instant::now());
            queue_server_datagram_responses(key.1, &mut state);
            let pending_capacity = server_datagram_pending_capacity_wait(key.1, &state);
            let has_pending_capacity = pending_capacity.is_some();
            let attachment_changes = slot.upgrade().map(|slot| slot.attachment_changes.clone());
            let has_attachment_changes = attachment_changes.is_some();
            tokio::select! {
                message = requests_rx.recv() => {
                    let Some(message) = message else {
                        break;
                    };
                    idle_deadline = Instant::now() + session_retention_timeout;
                    match message {
                        ServerDatagramWorkerMessage::Attach { commands, attachment, attached } => {
                            update_server_datagram_route(
                                &mut state,
                                commands,
                                attachment,
                            );
                            queue_server_datagram_responses(key.1, &mut state);
                            let _ = attached.send(());
                        }
                        ServerDatagramWorkerMessage::Request {
                            request,
                            commands,
                            attachment,
                            admission,
                        } => {
                            if admission.is_closed() {
                                continue;
                            }
                            let result = admit_server_datagram_request(
                                key.1,
                                &mut outbound_socket,
                                &mut state,
                                request,
                                commands,
                                attachment,
                                mux_limits,
                                &product_counter,
                            )
                            .await;
                            if let Err(error) = result.as_ref()
                                && let Some(lease) = gateway_lease.as_mut()
                                && let Err(feedback) =
                                    lease.completed(Some(error.to_string()))
                            {
                                crate::observability::process_event!(
                                    Warn,
                                    "udp_balancer",
                                    "server_flow_failure_feedback_failed",
                                    "balancer server UDP flow-failure feedback failed: {feedback}"
                                );
                            }
                            let _ = admission.send(result);
                        }
                        ServerDatagramWorkerMessage::ResponseFeedback { received } => {
                            acknowledge_server_datagram_responses(
                                &mut state,
                                &received,
                            );
                        }
                    }
                }
                received = async {
                    response_buffer.resize(response_buffer_len, 0);
                    outbound_socket.recv(&mut response_buffer[..]).await
                } => {
                    let len = match received {
                        Ok(len) => len,
                        Err(err) => {
                            failure = Some(err.to_string());
                            crate::observability::process_event!(
                                Warn,
                                "udp_outbound",
                                "receive_failed",
                                "UDP outbound receive failed: {err}"
                            );
                            send_server_datagram_close_to_routes(key.1, &state.routes);
                            break;
                        }
                    };
                    response_buffer.truncate(len);
                    let datagram_id = DatagramId(state.next_response_id);
                    state.next_response_id = match state.next_response_id.checked_add(1) {
                        Some(next) => next,
                        None => {
                            failure = Some("server UDP response ID exhausted".to_string());
                            send_server_datagram_close_to_routes(key.1, &state.routes);
                            break;
                        }
                    };
                    #[cfg(feature = "lab-diagnostics")]
                    lab_diagnostic(
                        "server_udp_outbound_response_received",
                        format_args!(
                            "flow_id={} datagram_id={} payload_bytes={}",
                            key.1.0,
                            datagram_id.0,
                            len,
                        ),
                    );
                    let payload = response_buffer.split_to(len).freeze();
                    let deadline = Instant::now() + session_retention_timeout;
                    cache_server_datagram_response(
                        &mut state,
                        CachedServerDatagram {
                            datagram_id,
                            deadline,
                            response: payload,
                            queued_once: false,
                        },
                        mux_limits,
                    );
                    product_counter.record_datagram_to_peer(len as u64);
                    queue_server_datagram_responses(key.1, &mut state);
                }
                _ = wait_for_server_datagram_route_capacity(pending_capacity), if has_pending_capacity => {
                    queue_server_datagram_responses(key.1, &mut state);
                }
                _ = wait_for_server_datagram_attachment_change(attachment_changes), if has_attachment_changes => {
                    let attachment_count = slot
                        .upgrade()
                        .map(|slot| slot.attachments.load(Ordering::Acquire))
                        .unwrap_or(0);
                    if attachment_count == 0 {
                        idle_deadline = Instant::now() + session_retention_timeout;
                    }
                }
                _ = tokio::time::sleep_until(idle_deadline) => {
                    let attachment_count = slot
                        .upgrade()
                        .map(|slot| slot.attachments.load(Ordering::Acquire))
                        .unwrap_or(0);
                    if attachment_count > 0 {
                        idle_deadline = Instant::now() + session_retention_timeout;
                        continue;
                    }
                    break;
                }
            }
        }
        if let Some(slot) = slot.upgrade() {
            ServerDatagramService::remove_flow_slot_if_current(&registry, key, &slot);
        }
        drop(realtime_registration);
        if let Some(lease) = gateway_lease.as_mut()
            && let Err(error) = lease.completed(failure.clone())
        {
            crate::observability::process_event!(
                Warn,
                "udp_balancer",
                "flow_outcome_feedback_failed",
                "balancer UDP flow-outcome feedback failed: {error}"
            );
        }
        if failure.is_none() {
            telemetry_flow.complete();
        }
    });
    requests_tx
}

#[allow(clippy::too_many_arguments)]
async fn admit_server_datagram_request(
    flow_id: DatagramFlowId,
    outbound_socket: &mut outbound::OutboundUdpSocket,
    state: &mut ServerDatagramFlowState,
    request: crate::runtime::path::ServerDatagramRequest,
    commands: ReliablePathCommandSender,
    attachment: Weak<()>,
    mux_limits: MuxLimits,
    product_counter: &crate::runtime::telemetry::ProductFlowCounter,
) -> Result<ServerDatagramSendOutcome, RuntimeError> {
    let now = Instant::now();
    prune_server_datagram_state(state, mux_limits, now);
    if request.ttl_ms == 0 {
        return Ok(ServerDatagramSendOutcome::Full);
    }
    update_server_datagram_route(state, commands, attachment);

    if request.payload.len() > mux_limits.max_payload_bytes {
        return Err(RuntimeError::Protocol(
            "datagram payload exceeds server limit",
        ));
    }
    let payload = DatagramPayloadIdentity::new(&request.payload);
    match state
        .received_requests
        .classify(request.datagram_id.0, payload)
    {
        Ok(DatagramAdmission::Duplicate) => {
            queue_server_datagram_responses(flow_id, state);
            return Ok(ServerDatagramSendOutcome::Accepted);
        }
        Ok(DatagramAdmission::Fresh) => {}
        Err(()) => {
            return Err(RuntimeError::Protocol(
                "datagram ID reused with a different payload",
            ));
        }
    }

    outbound_socket
        .send(&request.payload)
        .await
        .map_err(RuntimeError::OutboundConnect)?;
    #[cfg(feature = "lab-diagnostics")]
    lab_diagnostic(
        "server_udp_outbound_request_sent",
        format_args!(
            "flow_id={} datagram_id={} payload_bytes={}",
            flow_id.0,
            request.datagram_id.0,
            request.payload.len(),
        ),
    );
    product_counter.record_datagram_from_peer(request.payload.len() as u64);
    state
        .received_requests
        .record_fresh(request.datagram_id.0, payload);
    Ok(ServerDatagramSendOutcome::Accepted)
}

fn prune_server_datagram_state(
    state: &mut ServerDatagramFlowState,
    mux_limits: MuxLimits,
    now: Instant,
) {
    let mut retained = VecDeque::with_capacity(state.cached_responses.len());
    state.cached_response_bytes = 0;
    while let Some(response) = state.cached_responses.pop_front() {
        if response.deadline > now {
            state.cached_response_bytes = state
                .cached_response_bytes
                .saturating_add(response.response.len());
            retained.push_back(response);
        }
    }
    state.cached_responses = retained;
    while state.cached_responses.len() > mux_limits.max_ack_ranges.max(1)
        || state.cached_response_bytes > mux_limits.max_datagram_queue_bytes
    {
        let Some(expired) = state.cached_responses.pop_front() else {
            break;
        };
        state.cached_response_bytes = state
            .cached_response_bytes
            .saturating_sub(expired.response.len());
    }
    advance_server_datagram_route_floor(state);
}

fn cache_server_datagram_response(
    state: &mut ServerDatagramFlowState,
    response: CachedServerDatagram,
    mux_limits: MuxLimits,
) {
    state.cached_response_bytes = state
        .cached_response_bytes
        .saturating_add(response.response.len());
    state.cached_responses.push_back(response);
    prune_server_datagram_state(state, mux_limits, Instant::now());
}

fn acknowledge_server_datagram_responses(
    state: &mut ServerDatagramFlowState,
    received: &[OffsetRange],
) {
    let mut retained = VecDeque::with_capacity(state.cached_responses.len());
    state.cached_response_bytes = 0;
    while let Some(response) = state.cached_responses.pop_front() {
        if received.iter().any(|range| {
            response.datagram_id.0 >= range.start && response.datagram_id.0 < range.end
        }) {
            continue;
        }
        state.cached_response_bytes = state
            .cached_response_bytes
            .saturating_add(response.response.len());
        retained.push_back(response);
    }
    state.cached_responses = retained;
    advance_server_datagram_route_floor(state);
}

fn update_server_datagram_route(
    state: &mut ServerDatagramFlowState,
    commands: ReliablePathCommandSender,
    attachment: Weak<()>,
) {
    let existing = state
        .routes
        .iter()
        .position(|route| Weak::ptr_eq(&route.attachment, &attachment))
        .and_then(|position| state.routes.remove(position));
    state
        .routes
        .retain(|route| route.attachment.strong_count() > 0);
    let response_floor = state
        .cached_responses
        .front()
        .map(|response| response.datagram_id.0)
        .unwrap_or(state.next_response_id);
    state.routes.push_back(ServerDatagramRoute {
        commands,
        attachment,
        next_response_id: existing
            .map(|route| route.next_response_id)
            .unwrap_or(response_floor),
    });
    while state.routes.len() > MAX_DATAGRAM_RESPONSE_ROUTES {
        state.routes.pop_front();
    }
}

fn advance_server_datagram_route_floor(state: &mut ServerDatagramFlowState) {
    let response_floor = state
        .cached_responses
        .front()
        .map(|response| response.datagram_id.0)
        .unwrap_or(state.next_response_id);
    for route in &mut state.routes {
        route.next_response_id = route.next_response_id.max(response_floor);
    }
}

fn server_datagram_response_frame(
    flow_id: DatagramFlowId,
    response: &CachedServerDatagram,
) -> Frame {
    Frame::DatagramData {
        flow_id,
        datagram_id: response.datagram_id,
        ttl_ms: server_datagram_remaining_ttl_ms(response.deadline),
        payload: response.response.clone(),
    }
}

fn queue_server_datagram_responses(flow_id: DatagramFlowId, state: &mut ServerDatagramFlowState) {
    state
        .routes
        .retain(|route| route.attachment.strong_count() > 0);

    for response_index in 0..state.cached_responses.len() {
        if state.cached_responses[response_index].queued_once {
            continue;
        }
        let frame =
            server_datagram_response_frame(flow_id, &state.cached_responses[response_index]);
        let response_id = state.cached_responses[response_index].datagram_id.0;
        let mut queued = false;
        for route in state.routes.iter_mut().rev() {
            if try_send_server_datagram_realtime_frame(&route.commands, frame.clone()).is_ok() {
                route.next_response_id = response_id.saturating_add(1);
                queued = true;
                break;
            }
        }
        if queued {
            state.cached_responses[response_index].queued_once = true;
        } else {
            break;
        }
    }

    let Some(route) = state.routes.back_mut() else {
        return;
    };
    for response in &mut state.cached_responses {
        if response.datagram_id.0 < route.next_response_id {
            continue;
        }
        let frame = server_datagram_response_frame(flow_id, response);
        if try_send_server_datagram_realtime_frame(&route.commands, frame).is_err() {
            break;
        }
        route.next_response_id = response.datagram_id.0.saturating_add(1);
        response.queued_once = true;
    }
}

fn server_datagram_pending_capacity_wait(
    flow_id: DatagramFlowId,
    state: &ServerDatagramFlowState,
) -> Option<(Frame, Vec<ReliablePathCommandSender>)> {
    if let Some(response) = state
        .cached_responses
        .iter()
        .find(|response| !response.queued_once)
    {
        let routes = state
            .routes
            .iter()
            .filter(|route| route.attachment.strong_count() > 0)
            .map(|route| route.commands.clone())
            .collect::<Vec<_>>();
        return (!routes.is_empty())
            .then(|| (server_datagram_response_frame(flow_id, response), routes));
    }

    let latest = state
        .routes
        .back()
        .filter(|route| route.attachment.strong_count() > 0)?;
    let response = state
        .cached_responses
        .iter()
        .find(|response| response.datagram_id.0 >= latest.next_response_id)?;
    Some((
        server_datagram_response_frame(flow_id, response),
        vec![latest.commands.clone()],
    ))
}

async fn wait_for_server_datagram_route_capacity(
    pending: Option<(Frame, Vec<ReliablePathCommandSender>)>,
) {
    let Some((frame, routes)) = pending else {
        std::future::pending::<()>().await;
        return;
    };
    loop {
        let mut waits = routes
            .iter()
            .map(|route| Box::pin(route.capacity_notify().notified_owned()))
            .collect::<Vec<_>>();
        for wait in &mut waits {
            wait.as_mut().enable();
        }
        if routes
            .iter()
            .any(|route| route.can_enqueue_frame_now(&frame, TrafficClass::RealtimeDatagram))
        {
            return;
        }
        let _ = futures::future::select_all(waits).await;
    }
}

async fn wait_for_server_datagram_attachment_change(changes: Option<Arc<tokio::sync::Notify>>) {
    match changes {
        Some(changes) => changes.notified().await,
        None => std::future::pending::<()>().await,
    }
}

fn server_datagram_remaining_ttl_ms(deadline: Instant) -> u32 {
    let remaining = deadline.saturating_duration_since(Instant::now());
    if remaining.is_zero() {
        return 0;
    }
    remaining.as_millis().max(1).min(u128::from(u32::MAX)) as u32
}

fn send_server_datagram_close_to_routes(
    flow_id: DatagramFlowId,
    routes: &VecDeque<ServerDatagramRoute>,
) {
    for route in routes.iter().rev() {
        if route.attachment.upgrade().is_some()
            && try_send_server_datagram_realtime_frame(
                &route.commands,
                Frame::DatagramClose { flow_id },
            )
            .is_ok()
        {
            return;
        }
    }
}

pub(in crate::runtime) fn try_send_server_datagram_realtime_frame(
    commands: &ReliablePathCommandSender,
    frame: Frame,
) -> Result<(), RuntimeError> {
    debug_assert!(matches!(
        frame,
        Frame::DatagramData { .. } | Frame::DatagramFeedback { .. } | Frame::DatagramClose { .. }
    ));
    commands.try_enqueue_admitted_frame(frame, TrafficClass::RealtimeDatagram)
}

#[cfg(test)]
async fn send_server_datagram_realtime_frame_until(
    commands: &ReliablePathCommandSender,
    frame: Frame,
    deadline: Instant,
) -> Result<(), RuntimeError> {
    debug_assert!(matches!(frame, Frame::DatagramData { .. }));
    commands
        .enqueue_admitted_frame_until(frame, TrafficClass::RealtimeDatagram, deadline)
        .await
}

#[cfg(test)]
#[path = "tests_server.rs"]
mod tests;
