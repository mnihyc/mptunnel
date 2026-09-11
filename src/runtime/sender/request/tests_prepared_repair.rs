//! Actual weak-offer -> request Product claim tests. These prove the proposed
//! producer policy, not its performance or a violation of the previous RFC.

use super::*;
use crate::model::capacity::reliable_relay_buffer_len;
use crate::mux::stream::{ReliableSendStream, validate_stream_ack};
use crate::protocol::{StreamId, UnderlayProtocol};
use crate::runtime::path::authority::NativeCarrierRateAuthorityHandle;
use crate::runtime::path::commands::{
    ReliablePathCommand, ReliablePathCommandReceivers, ReliablePathCommandSender,
    reliable_path_command_channels, reliable_path_command_queue, try_recv_reliable_path_command,
};
use crate::runtime::path::prepared::PreparedOriginalWork;
use crate::runtime::sender::request::test_support::{
    client_test_context_with_paths, consume_client_path_proof_for_test,
    opened_test_relay_stream_with_native_source, seed_client_bulk_evidence_for_test,
};
use crate::runtime::sender::{ReliableRelaySenderQueue, RequestSenderService};
use crate::runtime::stream::{ReliableRelayAttachOutcome, ReliableRelayRemoteSet};
use crate::transport::RateHint;
use bytes::Bytes;

struct Fixture {
    context: ClientPathContext,
    product: SharedRequestProduct,
    target: RelayPathInstance,
    commands: ReliablePathCommandSender,
    receivers: ReliablePathCommandReceivers,
    _owner_receivers: ReliablePathCommandReceivers,
    native: Option<Arc<NativeCarrierRateAuthorityHandle>>,
    quantum: usize,
}

impl Fixture {
    fn new(target_underlay: UnderlayProtocol) -> Self {
        let context =
            client_test_context_with_paths(&["tcp://127.0.0.1:11811", "quic://127.0.0.1:11812"]);
        let stream_id = StreamId(811);
        let limits = context.mux_limits;
        let capacity = reliable_path_command_queue(limits);
        let owner_underlay = match target_underlay {
            UnderlayProtocol::Tcp => UnderlayProtocol::Udp,
            UnderlayProtocol::Udp => UnderlayProtocol::Tcp,
        };
        let (owner_commands, mut owner_receivers) = reliable_path_command_channels(capacity);
        let (opened, _) = opened_test_relay_stream_with_native_source(
            stream_id,
            owner_underlay,
            0,
            owner_commands,
            RateHint::BitsPerSecond(100_000_000),
            1,
            Some(100_000_000),
        );
        let (mut remotes, _remote_input) = ReliableRelayRemoteSet::new(opened, capacity);
        consume_client_path_proof_for_test(&mut owner_receivers);
        let owner = remotes.paths[0].instance();
        let (commands, mut receivers) = reliable_path_command_channels(capacity);
        let (opened, native) = opened_test_relay_stream_with_native_source(
            stream_id,
            target_underlay,
            0,
            commands.clone(),
            RateHint::BitsPerSecond(100_000_000),
            2,
            Some(100_000_000),
        );
        assert_eq!(remotes.attach(opened), ReliableRelayAttachOutcome::Attached);
        consume_client_path_proof_for_test(&mut receivers);
        let target = remotes.paths[1].instance();
        for instance in [owner, target] {
            seed_client_bulk_evidence_for_test(&context, instance);
        }
        let quantum = [owner, target]
            .into_iter()
            .map(|instance| {
                adaptive_reliable_relay_reinjection_bytes(
                    context.reliable_path_snapshot_for_instance(instance),
                    TrafficClass::Throughput,
                    limits,
                )
            })
            .max()
            .unwrap();
        assert!(quantum > 0 && quantum <= reliable_relay_buffer_len(limits));
        let mut send_stream = ReliableSendStream::new(stream_id, limits);
        let mut sender = RequestSenderService::new(stream_id);
        for _ in 0..3 {
            let frame = send_stream
                .send_data(Bytes::from(vec![0x81; quantum]))
                .unwrap();
            sender.record_original_frame_for_test(owner, &frame);
        }
        // Fixture ownership: the existing critical producer has already sent
        // the head. Extra service must neither revisit it nor wait for its ACK.
        let head = send_stream
            .first_retransmission_frame_for_range(
                OffsetRange {
                    start: 0,
                    end: quantum as u64,
                },
                quantum,
            )
            .unwrap();
        sender
            .multipath
            .record_reinjected_frame_for_test(target, &head);
        let product = SharedRequestProduct::new(RequestProductState {
            sender,
            send_stream,
            remotes,
            sender_queue: ReliableRelaySenderQueue::default(),
            last_send_ack: Default::default(),
            prepared: RequestPreparedSource::new(TrafficClass::Throughput, quantum),
        });
        {
            let mut state = product.lock();
            crate::runtime::relay::control::publish_prepared_request_work(
                &mut state,
                &product,
                &context,
                TrafficClass::Throughput,
                quantum,
                true,
            );
        }
        Self {
            context,
            product,
            target,
            commands,
            receivers,
            _owner_receivers: owner_receivers,
            native,
            quantum,
        }
    }

    fn offer(&mut self) -> PreparedOriginalWork {
        self.receivers
            .prepared_writer_ready_boundary(self.target.path_instance_id, true)
            .unwrap();
        let Some(ReliablePathCommand::PreparedOriginal(work)) =
            try_recv_reliable_path_command(&mut self.receivers)
        else {
            panic!("the actual actor must publish a weak repair offer");
        };
        assert!(work.is_repair());
        work
    }

    fn claim(&mut self, work: &PreparedOriginalWork) -> PreparedOriginalClaim {
        let ready = self
            .receivers
            .prepared_writer_ready_boundary(self.target.path_instance_id, true)
            .unwrap();
        work.try_claim(ready)
    }

    fn maturity_deadline(&self) -> Instant {
        {
            let mut state = self.product.lock();
            let range = OffsetRange {
                start: self.quantum as u64,
                end: (3 * self.quantum) as u64,
            };
            state
                .sender
                .multipath
                .observe_original_recovery_timing_for_range(range, |owner| {
                    self.context.reliable_path_snapshot_for_instance(owner)
                })
                .unwrap()
                .fallback_at
        }
    }

    async fn mature(&self) {
        tokio::time::sleep_until(self.maturity_deadline().into()).await;
    }

    fn debt(&self) -> usize {
        self.product
            .lock()
            .sender
            .multipath
            .accepted_reinjected_data_bytes(self.target)
    }
}

#[tokio::test]
async fn request_repair_immaturity_wait_uses_existing_assignment_deadline() {
    let mut fixture = Fixture::new(UnderlayProtocol::Udp);
    let deadline = fixture.maturity_deadline();
    let work = fixture.offer();
    let initial_debt = fixture.debt();
    match fixture.claim(&work) {
        PreparedOriginalClaim::Blocked(mut wait) => {
            assert_eq!(fixture.debt(), initial_debt);
            assert!(fixture.product.lock().sender_queue.is_empty());
            assert_eq!(fixture.commands.pending_bytes(), 0);
            let already_ready = futures::poll!(&mut wait).is_ready();
            if already_ready {
                assert!(Instant::now() >= deadline);
            }
            // No ACK, source publication or independent pacing timer is
            // supplied. The existing assignment deadline alone wakes this.
            if !already_ready {
                tokio::time::sleep_until(deadline.into()).await;
                assert!(futures::poll!(&mut wait).is_ready());
            }
            assert!(matches!(
                fixture.claim(&work),
                PreparedOriginalClaim::Claimed(_)
            ));
        }
        PreparedOriginalClaim::Claimed(_) => {
            // Host scheduling may carry this invocation across the deadline.
            assert!(Instant::now() >= deadline);
        }
        _ => panic!("current idle producer must either await maturity or commit after it"),
    }
}

#[tokio::test]
async fn request_repair_offer_preserves_reads_and_admits_successors_without_product_ack() {
    for underlay in [UnderlayProtocol::Tcp, UnderlayProtocol::Udp] {
        let mut fixture = Fixture::new(underlay);
        let before = {
            let state = fixture.product.lock();
            (
                state.send_stream.next_offset(),
                state.send_stream.reinjection_bytes(),
                crate::runtime::sender::reliable_relay_sender_queue_read_budget(
                    &state.send_stream,
                    &state.sender_queue,
                    fixture.context.mux_limits.max_repair_bytes,
                    reliable_relay_buffer_len(fixture.context.mux_limits),
                ),
            )
        };
        assert!(before.2 > 0);
        let work = fixture.offer();
        assert_eq!(
            fixture.commands.pending_bytes(),
            0,
            "offering repair stages no payload"
        );
        fixture.mature().await;
        let initial_debt = fixture.debt();
        let mut expected_start = fixture.quantum as u64;
        let mut accepted = 0usize;
        let mut work = Some(work);
        for _ in 0..2 {
            let current = work.take().unwrap();
            let PreparedOriginalClaim::Claimed(frame) = fixture.claim(&current) else {
                panic!("an idle exact writer must acquire its mature successor");
            };
            let (start, end, bytes) = reliable_stream_frame_extent(&frame).unwrap();
            assert_eq!(start, expected_start);
            assert!(end > start && bytes <= fixture.quantum);
            expected_start = end;
            accepted += bytes;
            // Same exact writer accounting used by ordinary Claimed frames.
            let charged = fixture.receivers.register_claimed_writer_frame(&frame);
            fixture.receivers.release_pending_command_bytes(charged);
            current.requeue();
            work = Some(fixture.offer());
        }
        assert_eq!(fixture.debt(), initial_debt + accepted);
        let state = fixture.product.lock();
        assert_eq!(
            (
                state.send_stream.next_offset(),
                state.send_stream.reinjection_bytes()
            ),
            (before.0, before.1)
        );
        assert!(state.sender_queue.is_empty());
        assert_eq!(
            state.prepared.last_claimed_at, None,
            "repair cannot invent Original source progress"
        );
        assert_eq!(
            state.sender.optional_reinjection.reinjected_bytes(),
            accepted as u64
        );
        assert_eq!(
            crate::runtime::sender::reliable_relay_sender_queue_read_budget(
                &state.send_stream,
                &state.sender_queue,
                fixture.context.mux_limits.max_repair_bytes,
                reliable_relay_buffer_len(fixture.context.mux_limits),
            ),
            before.2
        );
        assert_eq!(fixture.commands.pending_bytes(), 0);
    }
}

#[tokio::test]
async fn request_repair_refuses_busy_foreground_cancellation_and_fresh_receipt_without_copy() {
    for case in ["busy", "foreground", "cancel", "receipt", "native"] {
        let mut fixture = Fixture::new(UnderlayProtocol::Udp);
        fixture.mature().await;
        let work = fixture.offer();
        let original_debt = fixture.debt();
        match case {
            "busy" => {
                let product = fixture.product.clone();
                let guard = product.lock();
                assert!(matches!(
                    fixture.claim(&work),
                    PreparedOriginalClaim::Busy(_)
                ));
                drop(guard);
            }
            "foreground" | "cancel" | "receipt" => {
                let product = fixture.product.clone();
                let context = fixture.context.clone();
                fixture
                    .product
                    .before_prepared_native_resolve_once_for_test(move || {
                        let mut state = product.lock();
                        match case {
                            "foreground" => {
                                state
                                    .sender_queue
                                    .push_data(Bytes::from_static(b"foreground"));
                            }
                            "cancel" => {
                                state.prepared.claims_active = false;
                                state.prepared.registrations.clear();
                            }
                            "receipt" => {
                                let ack = validate_stream_ack(
                                    None,
                                    vec![OffsetRange {
                                        start: 0,
                                        end: state.send_stream.next_offset(),
                                    }],
                                    state.send_stream.next_offset(),
                                )
                                .unwrap();
                                let RequestProductState {
                                    sender,
                                    remotes,
                                    send_stream,
                                    ..
                                } = &mut *state;
                                sender
                                    .apply_request_product_ack(&context, remotes, send_stream, &ack)
                                    .unwrap();
                            }
                            _ => unreachable!(),
                        }
                        state.prepared.work_changed.notify_waiters();
                    });
                assert!(!matches!(
                    fixture.claim(&work),
                    PreparedOriginalClaim::Claimed(_)
                ));
            }
            "native" => {
                let native = fixture.native.clone().unwrap();
                fixture
                    .product
                    .before_prepared_native_resolve_once_for_test(move || {
                        native.advance_transport_activation_for_test(2).unwrap();
                        native
                            .publish_observation_for_test(2, 3, Some(100_000_000))
                            .unwrap();
                    });
                assert!(!matches!(
                    fixture.claim(&work),
                    PreparedOriginalClaim::Claimed(_)
                ));
            }
            _ => unreachable!(),
        }
        assert_eq!(
            fixture.debt(),
            if case == "receipt" { 0 } else { original_debt }
        );
        assert_eq!(fixture.commands.pending_bytes(), 0);
        assert_eq!(
            fixture
                .product
                .lock()
                .sender
                .optional_reinjection
                .reinjected_bytes(),
            0
        );
    }
}

#[tokio::test]
async fn request_repair_retries_after_own_carrier_foreground_drain() {
    let mut fixture = Fixture::new(UnderlayProtocol::Tcp);
    assert!(
        fixture.native.is_none(),
        "this wake cannot come from a QUIC Native cursor"
    );
    fixture.mature().await;
    let work = fixture.offer();
    let debt = fixture.debt();
    let model_generation = fixture.context.path_model_generation();
    let commands = fixture.commands.clone();
    let control = Frame::Ping { nonce: 811 };
    let published = control.clone();
    fixture
        .product
        .before_prepared_native_resolve_once_for_test(move || {
            // A different Product can publish carrier foreground after the offer
            // was dequeued. This changes no source, ACK, or path-model evidence.
            commands
                .try_enqueue_admitted_frame(published, TrafficClass::Control)
                .unwrap();
        });
    let PreparedOriginalClaim::Blocked(mut wait) = fixture.claim(&work) else {
        panic!("foreground publication must defeat final background consumption");
    };
    assert!(fixture.product.lock().sender_queue.is_empty());
    assert_eq!(fixture.debt(), debt);
    assert_eq!(
        fixture.commands.pending_bytes(),
        0,
        "Ping and offer own no payload charge"
    );
    assert!(fixture.commands.background_repair_ready().is_none());
    assert!(futures::poll!(&mut wait).is_pending());

    let mut own_boundary_changed = Box::pin(
        fixture
            .commands
            .writer_boundary()
            .change_notify()
            .notified_owned(),
    );
    own_boundary_changed.as_mut().enable();
    let foreground = try_recv_reliable_path_command(&mut fixture.receivers).unwrap();
    assert!(matches!(&foreground, ReliablePathCommand::SendFrame(frame) if frame == &control));
    // Exercise the exact receiver's writer occupation/completion transitions.
    // This is a retry-signal producer test, not a native wire-receipt claim.
    fixture.receivers.withdraw_writer_ready();
    let bytes = crate::runtime::path::commands::reliable_path_command_pending_bytes(&foreground);
    fixture.receivers.release_pending_command_bytes(bytes);
    drop(foreground);
    fixture
        .receivers
        .writer_ready_boundary(fixture.target.path_instance_id)
        .unwrap();
    assert!(futures::poll!(&mut own_boundary_changed).is_ready());
    assert!(fixture.commands.background_repair_ready().is_some());
    assert_eq!(fixture.context.path_model_generation(), model_generation);
    assert_eq!(fixture.debt(), debt);
    let retry_signaled = futures::poll!(&mut wait).is_ready();
    if !retry_signaled {
        // Positive control distinguishes a missing own-writer dependency from
        // a cancelled or incorrectly polled callback wait.
        let changed = fixture.product.lock().prepared.work_changed.clone();
        changed.notify_waiters();
        assert!(
            futures::poll!(&mut wait).is_ready(),
            "Product notification reaches the same live wait"
        );
    }
    assert!(
        retry_signaled,
        "completed same-carrier foreground and renewed Ready must wake the parked repair without an unrelated event"
    );
}

#[tokio::test]
async fn request_repair_ready_cleanup_does_not_regenerate_refused_work() {
    for underlay in [UnderlayProtocol::Tcp, UnderlayProtocol::Udp] {
        let mut fixture = Fixture::new(underlay);
        let work = fixture.offer();
        fixture
            .product
            .lock()
            .sender_queue
            .push_data(Bytes::from_static(b"staged Original"));
        let debt = fixture.debt();
        let model_generation = fixture.context.path_model_generation();
        let previous_ready = fixture.commands.writer_boundary().snapshot().unwrap();
        let PreparedOriginalClaim::Blocked(mut wait) = fixture.claim(&work) else {
            panic!("staged foreground must refuse repair before any maturity timer");
        };
        assert!(futures::poll!(&mut wait).is_pending());
        // No foreground command completed and no source/Native state changed.
        // Ordinary Ready cleanup and renewal alone cannot authorize retry of
        // the same refused metadata; subscribe to foreground release instead.
        fixture.receivers.withdraw_writer_ready();
        let current_ready = fixture
            .receivers
            .writer_ready_boundary(fixture.target.path_instance_id)
            .unwrap()
            .receipt();
        assert_ne!(previous_ready, current_ready);
        assert!(
            futures::poll!(&mut wait).is_pending(),
            "own Ready withdrawal/renewal must not self-trigger a refused repair"
        );
        assert_eq!(fixture.context.path_model_generation(), model_generation);
        assert_eq!(fixture.debt(), debt);
        assert_eq!(fixture.commands.pending_bytes(), 0);
    }
}

#[tokio::test]
async fn request_repair_uncovered_head_and_covered_expired_successor_keep_critical_authority() {
    let mut fixture = Fixture::new(UnderlayProtocol::Tcp);
    fixture.mature().await;
    let work = fixture.offer();
    {
        let mut state = fixture.product.lock();
        let ack = validate_stream_ack(
            None,
            vec![OffsetRange {
                start: 0,
                end: fixture.quantum as u64,
            }],
            state.send_stream.next_offset(),
        )
        .unwrap();
        let RequestProductState {
            sender,
            remotes,
            send_stream,
            ..
        } = &mut *state;
        sender
            .apply_request_product_ack(&fixture.context, remotes, send_stream, &ack)
            .unwrap();
    }
    assert!(
        matches!(fixture.claim(&work), PreparedOriginalClaim::Blocked(_)),
        "new uncovered logical head belongs to critical recovery"
    );
    assert_eq!(fixture.debt(), 0);
    let copy_deadline = {
        let mut state = fixture.product.lock();
        let frame = state
            .send_stream
            .first_retransmission_frame_for_range(
                OffsetRange {
                    start: fixture.quantum as u64,
                    end: (3 * fixture.quantum) as u64,
                },
                2 * fixture.quantum,
            )
            .unwrap();
        state
            .sender
            .multipath
            .record_reinjected_frame_for_test(fixture.target, &frame);
        state
            .sender
            .reinjection_suppression_deadline_for_frame(&frame, &state.remotes)
            .unwrap()
    };
    tokio::time::sleep_until(copy_deadline.into()).await;
    // The accepted copy may outlive its suppression deadline. It still covers
    // its exact bytes; only the next uncovered successor can be acquired.
    let PreparedOriginalClaim::Claimed(frame) = fixture.claim(&work) else {
        panic!("remaining uncovered successor");
    };
    assert_eq!(
        reliable_stream_frame_extent(&frame).unwrap().0,
        (2 * fixture.quantum) as u64
    );
}
