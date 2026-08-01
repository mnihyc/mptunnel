use super::*;
use crate::config::{ClientSecurityConfig, SharedSecret};
use crate::performance::ResourceLimits;
use crate::protocol::{OffsetRange, UnderlayProtocol};
use crate::runtime::path::ClientPathContext;
use crate::scheduler::TrafficClass;
use crate::transport::PathSpec;
use crate::transport::encrypted::EncryptedFramedStream;
use bytes::Bytes;
use std::time::Duration;
use tokio::net::TcpListener;

struct ValidationWirePeer {
    frames: mpsc::Sender<Frame>,
    observed: mpsc::Receiver<Frame>,
    task: tokio::task::JoinHandle<Result<(), RuntimeError>>,
}

async fn validation_wire_peer(peer_usage: PathUsage) -> (PathSpec, ValidationWirePeer) {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind validation wire peer");
    let address = listener.local_addr().expect("validation wire peer address");
    let path = format!("tcp://{address}?tcp-carriers=1-3")
        .parse()
        .expect("validation TCP path");
    let (frames, mut frame_commands) = mpsc::channel(16);
    let (observed_frames, observed) = mpsc::channel(16);
    let task = tokio::spawn(async move {
        let (stream, _) = listener.accept().await?;
        let mut framed = EncryptedFramedStream::accept(
            stream,
            &crate::transport::encrypted::test_server_tls_config(),
            crate::protocol::codec::CodecLimits::default(),
        )
        .await?;
        let _admission = framed.read_tcp_admission().await?;
        let (_session_id, path_id) = match framed.read_frame().await? {
            Frame::PathJoin {
                session_id,
                path_id,
                underlay: UnderlayProtocol::Tcp,
                purpose: PathPurpose::Validation,
                ..
            } => (session_id, path_id),
            _ => {
                return Err(RuntimeError::Protocol(
                    "validation actor did not send validation-purpose PATH_JOIN",
                ));
            }
        };
        match framed.read_frame().await? {
            Frame::PathStatus {
                path_id: status_path_id,
                sequence: 0,
                ..
            } if status_path_id == path_id => {}
            _ => {
                return Err(RuntimeError::Protocol(
                    "validation actor did not send initial PATH_STATUS",
                ));
            }
        }
        framed
            .write_frames(&[
                Frame::SessionReady,
                Frame::PathStatus {
                    path_id,
                    sequence: 0,
                    usage: peer_usage,
                },
            ])
            .await?;
        framed.flush().await?;
        let (mut reader, mut writer) = framed.split()?;
        loop {
            tokio::select! {
                frame = reader.read_frame() => {
                    let frame = frame?;
                    if observed_frames.send(frame).await.is_err() {
                        return Ok(());
                    }
                }
                frame = frame_commands.recv() => {
                    let Some(frame) = frame else {
                        return Ok(());
                    };
                    writer.write_frame(&frame).await?;
                    writer.flush().await?;
                }
            }
        }
    });
    (
        path,
        ValidationWirePeer {
            frames,
            observed,
            task,
        },
    )
}

fn client_security() -> ClientSecurityConfig {
    ClientSecurityConfig::for_test(
        SharedSecret::new(b"0123456789abcdef0123456789abcdef".to_vec())
            .expect("validation test secret"),
    )
}

async fn validation_actor(
    path: PathSpec,
    validation_id: NonZeroU64,
    stream_id: StreamId,
) -> (
    ClientPathContext,
    ClientTcpValidationController,
    mpsc::Receiver<ClientTcpValidationEvent>,
    tokio::task::JoinHandle<Result<(), RuntimeError>>,
) {
    validation_actor_with_retention(
        path,
        validation_id,
        stream_id,
        crate::config::DEFAULT_SESSION_RETENTION_TIMEOUT,
    )
    .await
}

async fn validation_actor_with_retention(
    path: PathSpec,
    validation_id: NonZeroU64,
    stream_id: StreamId,
    retention_timeout: Duration,
) -> (
    ClientPathContext,
    ClientTcpValidationController,
    mpsc::Receiver<ClientTcpValidationEvent>,
    tokio::task::JoinHandle<Result<(), RuntimeError>>,
) {
    let context = ClientPathContext::new(vec![path], client_security(), ResourceLimits::default())
        .expect("validation client context");
    let config_index = context
        .tcp_config_index(0)
        .expect("validation TCP config index");
    let endpoint_generation = context
        .tcp_carrier_groups
        .endpoint_policy(config_index)
        .expect("validation endpoint policy")
        .snapshot()
        .generation;
    let reservation = context
        .tcp_carrier_groups
        .reserve(config_index)
        .expect("reserve elastic validation carrier");
    let mut admission = context.tcp_sessions[0]
        .c2s_validation_admission_for_test(
            reservation,
            endpoint_generation,
            validation_id,
            stream_id,
            tokio::time::Instant::now() + Duration::from_secs(5),
        )
        .expect("create C2S validation admission");
    admission.runtime.session_retention_timeout = retention_timeout;
    let (session, controller, events) = ClientTcpValidationSession::new(admission);
    let task = tokio::spawn(session.run());
    (context, controller, events, task)
}

struct ServerToClientValidationActor {
    context: ClientPathContext,
    _minimum: ClientTcpCarrierReservation,
    controller: ClientTcpValidationController,
    events: mpsc::Receiver<ClientTcpValidationEvent>,
    task: tokio::task::JoinHandle<Result<(), RuntimeError>>,
}

async fn server_to_client_validation_actor(
    path: PathSpec,
    request_id: NonZeroU64,
    stream_id: StreamId,
) -> ServerToClientValidationActor {
    let context = ClientPathContext::new(vec![path], client_security(), ResourceLimits::default())
        .expect("validation client context");
    let config_index = context
        .tcp_config_index(0)
        .expect("validation TCP config index");
    let minimum = context
        .tcp_carrier_groups
        .reserve(config_index)
        .expect("reserve configured minimum carrier");
    let demand = crate::runtime::path::tcp::service::ClientTcpCarrierDemand {
        request_id,
        stream_id: Some(stream_id),
    };
    context
        .state
        .tcp_carrier_service()
        .apply_server_demand(demand)
        .expect("publish exact server demand");
    let admission = context
        .claim_server_to_client_tcp_carrier(demand)
        .expect("claim bounded S2C carrier");
    let actor_admission = context.tcp_sessions[0]
        .s2c_validation_admission(
            admission,
            tokio::time::Instant::now() + Duration::from_secs(5),
        )
        .expect("create S2C validation admission");
    let (session, controller, events) = ClientTcpValidationSession::new(actor_admission);
    let task = tokio::spawn(session.run());
    ServerToClientValidationActor {
        context,
        _minimum: minimum,
        controller,
        events,
        task,
    }
}

async fn next_wire_frame(peer: &mut ValidationWirePeer) -> Frame {
    tokio::time::timeout(Duration::from_secs(5), peer.observed.recv())
        .await
        .expect("validation wire frame timed out")
        .expect("validation wire peer ended")
}

async fn next_actor_event(
    events: &mut mpsc::Receiver<ClientTcpValidationEvent>,
) -> ClientTcpValidationEvent {
    tokio::time::timeout(Duration::from_secs(5), events.recv())
        .await
        .expect("validation actor event timed out")
        .expect("validation actor event channel ended")
}

async fn next_server_to_client_actor_event(
    actor: &mut ServerToClientValidationActor,
) -> ClientTcpValidationEvent {
    match tokio::time::timeout(Duration::from_secs(5), actor.events.recv())
        .await
        .expect("S2C validation actor event timed out")
    {
        Some(event) => event,
        None => {
            let result = (&mut actor.task)
                .await
                .expect("S2C validation actor task panicked");
            panic!("S2C validation actor ended before its event: {result:?}");
        }
    }
}

async fn finish_actor(task: tokio::task::JoinHandle<Result<(), RuntimeError>>) {
    tokio::time::timeout(Duration::from_secs(5), task)
        .await
        .expect("validation actor did not finish")
        .expect("validation actor task")
        .expect("validation actor result");
}

fn stop_wire_peer(peer: ValidationWirePeer) {
    peer.task.abort();
}

#[tokio::test]
async fn retained_c2s_validation_hands_off_exact_transport_without_ordinary_publication() {
    let validation_id = NonZeroU64::new(1).expect("nonzero validation ID");
    let stream_id = StreamId(41);
    let (path, mut peer) = validation_wire_peer(PathUsage::Available).await;
    let (context, controller, mut events, actor) =
        validation_actor(path, validation_id, stream_id).await;

    assert_eq!(
        next_wire_frame(&mut peer).await,
        Frame::TcpCarrierValidate {
            validation_id: validation_id.get(),
            request_id: 0,
            direction: PathMetricDirection::ClientToServer,
            stream_id,
        }
    );
    let (candidate, validation_data) = match next_actor_event(&mut events).await {
        ClientTcpValidationEvent::Admitted {
            candidate,
            validation_data,
        } => (candidate, validation_data),
        _ => panic!("expected admitted validation candidate"),
    };
    assert_eq!(candidate.validation_id, validation_id);
    assert_eq!(candidate.stream_id, stream_id);
    assert_eq!(context.tcp_carrier_groups.occupied(0), Some(1));
    assert_eq!(context.tcp_sessions[0].connection_instance_id(), None);
    assert_eq!(context.authenticated_carriers.snapshot().live_count, 0);
    assert!(!context.authenticated_carriers.snapshot().ever_authenticated);

    validation_data
        .try_reserve_tcp_carrier_validation_data(
            validation_id,
            Frame::StreamData {
                stream_id,
                offset: 0,
                payload: Bytes::from_static(b"candidate"),
            },
            TrafficClass::Throughput,
        )
        .expect("reserve validation data")
        .commit();
    let boundary_requested_at = std::time::Instant::now();
    let boundary_completed_at = controller
        .writer_boundary()
        .await
        .expect("serialize exact candidate writer boundary");
    assert!(boundary_completed_at >= boundary_requested_at);
    assert_eq!(
        next_wire_frame(&mut peer).await,
        Frame::StreamData {
            stream_id,
            offset: 0,
            payload: Bytes::from_static(b"candidate"),
        }
    );
    let feedback = Frame::StreamAck {
        stream_id,
        complete: false,
        ranges: vec![OffsetRange { start: 0, end: 9 }],
    };
    peer.frames
        .send(feedback.clone())
        .await
        .expect("send validation feedback");
    match next_actor_event(&mut events).await {
        ClientTcpValidationEvent::Control {
            candidate: event_candidate,
            frame,
        } => {
            assert_eq!(event_candidate, candidate);
            assert_eq!(frame, feedback);
        }
        _ => panic!("expected exact target control"),
    }

    controller
        .serialize_result(TcpCarrierValidationResult::Retain)
        .await
        .expect("serialize RETAIN result");
    assert_eq!(
        next_wire_frame(&mut peer).await,
        Frame::TcpCarrierResult {
            validation_id: validation_id.get(),
            direction: PathMetricDirection::ClientToServer,
            result: TcpCarrierValidationResult::Retain,
        }
    );
    peer.frames
        .send(Frame::TcpCarrierResultAck {
            validation_id: validation_id.get(),
            direction: PathMetricDirection::ClientToServer,
            result: TcpCarrierValidationResult::Retain,
        })
        .await
        .expect("acknowledge RETAIN result");
    let handoff = match next_actor_event(&mut events).await {
        ClientTcpValidationEvent::Retained(handoff) => *handoff,
        _ => panic!("expected retained transport handoff"),
    };
    assert_eq!(handoff.candidate, candidate);
    assert_eq!(handoff.reservation.path_id(), candidate.path_id);
    assert_eq!(handoff.runtime.runtime.path_id(), candidate.path_id);
    assert_eq!(handoff.runtime.runtime.purpose, PathPurpose::Validation);
    assert_eq!(
        handoff.runtime.runtime.remote_port,
        Some(candidate.remote_port)
    );
    assert_eq!(handoff.connection.path_id, candidate.path_id);
    assert_eq!(handoff.connection.purpose, PathPurpose::Validation);
    finish_actor(actor).await;

    assert_eq!(context.tcp_sessions[0].connection_instance_id(), None);
    assert_eq!(context.authenticated_carriers.snapshot().live_count, 0);
    assert!(!context.authenticated_carriers.snapshot().ever_authenticated);
    assert_eq!(context.tcp_carrier_groups.occupied(0), Some(1));
    drop(handoff);
    assert_eq!(context.tcp_carrier_groups.occupied(0), Some(0));
    stop_wire_peer(peer);
}

#[tokio::test]
async fn negative_c2s_validation_waits_for_exact_ack_and_zero_work_before_ordered_drain() {
    let validation_id = NonZeroU64::new(7).expect("nonzero validation ID");
    let stream_id = StreamId(73);
    let (path, mut peer) = validation_wire_peer(PathUsage::Available).await;
    let (context, controller, mut events, actor) =
        validation_actor(path, validation_id, stream_id).await;
    let validate = next_wire_frame(&mut peer).await;
    assert!(matches!(
        validate,
        Frame::TcpCarrierValidate {
            validation_id: 7,
            request_id: 0,
            direction: PathMetricDirection::ClientToServer,
            stream_id: StreamId(73),
        }
    ));
    let candidate = match next_actor_event(&mut events).await {
        ClientTcpValidationEvent::Admitted { candidate, .. } => candidate,
        _ => panic!("expected admitted validation candidate"),
    };

    controller
        .serialize_result(TcpCarrierValidationResult::NoGain)
        .await
        .expect("serialize NO_GAIN result");
    assert_eq!(
        next_wire_frame(&mut peer).await,
        Frame::TcpCarrierResult {
            validation_id: validation_id.get(),
            direction: PathMetricDirection::ClientToServer,
            result: TcpCarrierValidationResult::NoGain,
        }
    );
    peer.frames
        .send(Frame::TcpCarrierResultAck {
            validation_id: validation_id.get(),
            direction: PathMetricDirection::ClientToServer,
            result: TcpCarrierValidationResult::NoGain,
        })
        .await
        .expect("acknowledge NO_GAIN result");
    match next_actor_event(&mut events).await {
        ClientTcpValidationEvent::ResultAcknowledged {
            candidate: event_candidate,
            result: TcpCarrierValidationResult::NoGain,
        } => assert_eq!(event_candidate, candidate),
        _ => panic!("expected exact negative-result acknowledgment"),
    }
    assert!(
        peer.observed.try_recv().is_err(),
        "negative acknowledgment alone must not serialize PATH_DRAIN"
    );

    controller
        .confirm_candidate_work_zero()
        .await
        .expect("confirm candidate zero-work boundary");
    assert_eq!(
        next_wire_frame(&mut peer).await,
        Frame::StreamDetach { stream_id }
    );
    assert_eq!(
        next_wire_frame(&mut peer).await,
        Frame::PathDrain {
            path_id: candidate.path_id,
        }
    );
    peer.frames
        .send(Frame::PathClose {
            path_id: candidate.path_id,
            reason: CloseReason::Normal,
        })
        .await
        .expect("complete ordered validation drain");
    match next_actor_event(&mut events).await {
        ClientTcpValidationEvent::Drained {
            candidate: event_candidate,
        } => assert_eq!(event_candidate, candidate),
        _ => panic!("expected ordered validation drain completion"),
    }
    finish_actor(actor).await;
    assert_eq!(context.tcp_carrier_groups.occupied(0), Some(0));
    stop_wire_peer(peer);
}

#[tokio::test]
async fn expired_c2s_validation_serializes_withdrawn_before_exact_native_retirement() {
    let validation_id = NonZeroU64::new(11).expect("nonzero validation ID");
    let stream_id = StreamId(79);
    let (path, mut peer) = validation_wire_peer(PathUsage::Available).await;
    let (context, _controller, mut events, actor) =
        validation_actor_with_retention(path, validation_id, stream_id, Duration::from_millis(500))
            .await;

    assert_eq!(
        next_wire_frame(&mut peer).await,
        Frame::TcpCarrierValidate {
            validation_id: validation_id.get(),
            request_id: 0,
            direction: PathMetricDirection::ClientToServer,
            stream_id,
        }
    );
    assert!(matches!(
        next_actor_event(&mut events).await,
        ClientTcpValidationEvent::Admitted { .. }
    ));
    assert_eq!(
        next_wire_frame(&mut peer).await,
        Frame::TcpCarrierResult {
            validation_id: validation_id.get(),
            direction: PathMetricDirection::ClientToServer,
            result: TcpCarrierValidationResult::Withdrawn,
        }
    );

    let result = tokio::time::timeout(Duration::from_secs(5), actor)
        .await
        .expect("expired validation actor did not finish")
        .expect("expired validation actor task");
    assert!(matches!(result, Err(RuntimeError::SessionRetentionTimeout)));
    assert_eq!(context.tcp_carrier_groups.occupied(0), Some(0));
    stop_wire_peer(peer);
}

#[tokio::test]
async fn retained_s2c_validation_commits_exact_authority_before_wire_ack() {
    let request_id = NonZeroU64::new(17).expect("nonzero request ID");
    let stream_id = StreamId(83);
    let (path, mut peer) = validation_wire_peer(PathUsage::Available).await;
    let mut actor = server_to_client_validation_actor(path, request_id, stream_id).await;

    let validation_id = match next_wire_frame(&mut peer).await {
        Frame::TcpCarrierValidate {
            validation_id,
            request_id: received_request_id,
            direction: PathMetricDirection::ServerToClient,
            stream_id: received_stream_id,
        } if received_request_id == request_id.get() && received_stream_id == stream_id => {
            NonZeroU64::new(validation_id).expect("nonzero validation ID")
        }
        _ => panic!("expected exact S2C validation request"),
    };
    let candidate = match next_server_to_client_actor_event(&mut actor).await {
        ClientTcpValidationEvent::ReceiverAdmitted { candidate } => candidate,
        _ => panic!("expected receiver-side validation admission"),
    };
    assert_eq!(candidate.validation_id, validation_id);
    assert!(!actor.context.tcp_retained_carriers.direction_authorized(
        candidate.instance.key,
        candidate.instance.path_instance_id,
        PathMetricDirection::ServerToClient,
    ));

    let candidate_data = Frame::StreamData {
        stream_id,
        offset: 0,
        payload: Bytes::from_static(b"candidate"),
    };
    peer.frames
        .send(candidate_data.clone())
        .await
        .expect("send S2C candidate data");
    match next_server_to_client_actor_event(&mut actor).await {
        ClientTcpValidationEvent::Control {
            candidate: event_candidate,
            frame,
        } => {
            assert_eq!(event_candidate, candidate);
            assert_eq!(frame, candidate_data);
        }
        _ => panic!("expected exact S2C candidate data"),
    }

    peer.frames
        .send(Frame::TcpCarrierResult {
            validation_id: validation_id.get(),
            direction: PathMetricDirection::ServerToClient,
            result: TcpCarrierValidationResult::Retain,
        })
        .await
        .expect("send S2C RETAIN result");
    assert!(matches!(
        next_server_to_client_actor_event(&mut actor).await,
        ClientTcpValidationEvent::ResultReceived {
            candidate: event_candidate,
            result: TcpCarrierValidationResult::Retain,
        } if event_candidate == candidate
    ));
    actor
        .controller
        .acknowledge_server_to_client_result(TcpCarrierValidationResult::Retain)
        .await
        .expect("acknowledge S2C RETAIN");
    assert!(actor.context.tcp_retained_carriers.direction_authorized(
        candidate.instance.key,
        candidate.instance.path_instance_id,
        PathMetricDirection::ServerToClient,
    ));
    assert_eq!(
        next_wire_frame(&mut peer).await,
        Frame::TcpCarrierResultAck {
            validation_id: validation_id.get(),
            direction: PathMetricDirection::ServerToClient,
            result: TcpCarrierValidationResult::Retain,
        }
    );
    let handoff = match next_server_to_client_actor_event(&mut actor).await {
        ClientTcpValidationEvent::Retained(handoff) => handoff,
        _ => panic!("expected S2C retained handoff"),
    };
    assert!(handoff.server_to_client.is_some());
    finish_actor(actor.task).await;
    drop(handoff);
    assert!(!actor.context.tcp_retained_carriers.direction_authorized(
        candidate.instance.key,
        candidate.instance.path_instance_id,
        PathMetricDirection::ServerToClient,
    ));
    assert_eq!(actor.context.tcp_carrier_groups.occupied(0), Some(1));
    stop_wire_peer(peer);
}

#[tokio::test]
async fn negative_s2c_validation_ack_precedes_ordered_candidate_drain() {
    let request_id = NonZeroU64::new(19).expect("nonzero request ID");
    let stream_id = StreamId(89);
    let (path, mut peer) = validation_wire_peer(PathUsage::Available).await;
    let mut actor = server_to_client_validation_actor(path, request_id, stream_id).await;
    let validation_id = match next_wire_frame(&mut peer).await {
        Frame::TcpCarrierValidate { validation_id, .. } => {
            NonZeroU64::new(validation_id).expect("nonzero validation ID")
        }
        _ => panic!("expected S2C validation request"),
    };
    let candidate = match next_server_to_client_actor_event(&mut actor).await {
        ClientTcpValidationEvent::ReceiverAdmitted { candidate } => candidate,
        _ => panic!("expected receiver-side validation admission"),
    };
    peer.frames
        .send(Frame::TcpCarrierResult {
            validation_id: validation_id.get(),
            direction: PathMetricDirection::ServerToClient,
            result: TcpCarrierValidationResult::NoGain,
        })
        .await
        .expect("send S2C NO_GAIN result");
    assert!(matches!(
        next_server_to_client_actor_event(&mut actor).await,
        ClientTcpValidationEvent::ResultReceived {
            result: TcpCarrierValidationResult::NoGain,
            ..
        }
    ));
    actor
        .controller
        .acknowledge_server_to_client_result(TcpCarrierValidationResult::NoGain)
        .await
        .expect("acknowledge S2C NO_GAIN");
    assert_eq!(
        next_wire_frame(&mut peer).await,
        Frame::TcpCarrierResultAck {
            validation_id: validation_id.get(),
            direction: PathMetricDirection::ServerToClient,
            result: TcpCarrierValidationResult::NoGain,
        }
    );
    assert_eq!(
        next_wire_frame(&mut peer).await,
        Frame::StreamDetach { stream_id }
    );
    assert_eq!(
        next_wire_frame(&mut peer).await,
        Frame::PathDrain {
            path_id: candidate.path_id,
        }
    );
    peer.frames
        .send(Frame::PathClose {
            path_id: candidate.path_id,
            reason: CloseReason::Normal,
        })
        .await
        .expect("complete S2C candidate drain");
    assert!(matches!(
        next_server_to_client_actor_event(&mut actor).await,
        ClientTcpValidationEvent::Drained {
            candidate: event_candidate,
        } if event_candidate == candidate
    ));
    finish_actor(actor.task).await;
    assert_eq!(actor.context.tcp_carrier_groups.occupied(0), Some(1));
    stop_wire_peer(peer);
}
