//! Detached native observations for an imminent response Original claim.

use super::snapshot::{
    server_bulk_output_snapshot_at, server_native_bulk_output_snapshot_at,
    server_sender_path_target_at,
};
use super::{ResponseAcquisitionOutputId, ResponseStreamBinding, ResponseStreamOutputs};
use crate::model::carrier_rate_authority::CarrierRateAuthorityScope;
use crate::protocol::{PathMetricDirection, UnderlayProtocol};
use crate::runtime::path::authority::NativeCarrierSchedulingShapeSnapshot;
use crate::runtime::path::commands::ReliablePathCommandSender;
use crate::scheduler::TrafficClass;
use std::sync::atomic::Ordering;
use std::time::Instant;

#[derive(Clone)]
pub(in crate::runtime) struct ResponsePreparedOutput {
    pub(in crate::runtime) identity: ResponseAcquisitionOutputId,
    pub(in crate::runtime) commands: ReliablePathCommandSender,
}

pub(in crate::runtime) struct ResponsePreparedNativeInputs {
    outputs: Vec<(
        ResponsePreparedOutput,
        Option<NativeCarrierSchedulingShapeSnapshot>,
    )>,
}

impl ResponsePreparedNativeInputs {
    /// This is the only native read in the claim's advisory observation. The
    /// caller has released source ownership before resolving these handles.
    pub(in crate::runtime) fn resolve(outputs: Vec<ResponsePreparedOutput>) -> Self {
        Self {
            outputs: outputs
                .into_iter()
                .map(|output| {
                    let shape = output
                        .commands
                        .native_rate_authority()
                        .and_then(|authority| {
                            authority
                                .scheduling_shape_snapshot(CarrierRateAuthorityScope::new(
                                    output.identity.path_instance_id,
                                    PathMetricDirection::ServerToClient,
                                ))
                                .ok()
                        });
                    (output, shape)
                })
                .collect(),
        }
    }

    pub(in crate::runtime) fn replace_fenced_target(
        &mut self,
        identity: ResponseAcquisitionOutputId,
        shape: NativeCarrierSchedulingShapeSnapshot,
    ) {
        for (output, current) in &mut self.outputs {
            if output.identity == identity {
                *current = Some(shape);
            }
        }
    }
}

pub(in crate::runtime) struct ResponsePreparedObservation<'binding> {
    // Borrowing the owner bounds reuse to its lifetime; no raw-address cache.
    binding: &'binding ResponseStreamBinding,
    next_offset: u64,
    pub(in crate::runtime) debt_projection: super::ResponseDebtProjection,
    pub(in crate::runtime) model_generation: u64,
}

impl ResponseStreamBinding {
    /// Exact handles only: no Native acquisition or duplicate mutable model.
    pub(in crate::runtime) fn prepared_outputs(&self) -> Vec<ResponsePreparedOutput> {
        let outputs = self
            .outputs
            .lock()
            .expect("server reliable stream binding lock");
        if !self.response_stream_open.load(Ordering::Acquire) {
            return Vec::new();
        }
        outputs
            .entries
            .iter()
            .map(|entry| ResponsePreparedOutput {
                identity: ResponseAcquisitionOutputId::from(entry),
                commands: entry.commands.clone(),
            })
            .collect()
    }

    /// Re-project current Product fields from supplied Native values. A full
    /// membership receipt prevents pairing an old physical handle with a new
    /// incarnation. Unselected Ready changes are not ownership invalidations.
    pub(in crate::runtime) fn observe_prepared_original(
        &self,
        inputs: &ResponsePreparedNativeInputs,
        lane: TrafficClass,
        next_offset: u64,
    ) -> Option<ResponsePreparedObservation<'_>> {
        self.observe_prepared_original_with_prior(inputs, lane, next_offset, None)
    }

    /// The advisory and final observations belong to one synchronous claim.
    /// Only lower-range facts may be reused. Targets are rebuilt from current
    /// Product/path state and the supplied Native inputs; the claim refreshes
    /// its selected Native shape under the existing final fence. Selection,
    /// recovery priority and accepted ownership are checked again. Consuming
    /// the prior observation transfers its numeric vector without cloning it.
    pub(in crate::runtime) fn reobserve_prepared_original(
        &self,
        inputs: &ResponsePreparedNativeInputs,
        lane: TrafficClass,
        next_offset: u64,
        prior: ResponsePreparedObservation<'_>,
    ) -> Option<ResponsePreparedObservation<'_>> {
        self.observe_prepared_original_with_prior(inputs, lane, next_offset, Some(prior))
    }

    fn observe_prepared_original_with_prior(
        &self,
        inputs: &ResponsePreparedNativeInputs,
        lane: TrafficClass,
        next_offset: u64,
        prior: Option<ResponsePreparedObservation<'_>>,
    ) -> Option<ResponsePreparedObservation<'_>> {
        let outputs = self
            .outputs
            .lock()
            .expect("server reliable stream binding lock");
        let targets = self.prepared_targets_locked(&outputs, inputs, lane)?;
        let generation = self.response_model_generation.load(Ordering::Acquire);
        // Keep the cheap single-output path on its established computation.
        // Changes to the ledger/hole selection publish this generation under
        // the same outputs lock. Timing-only mutations do not alter these facts.
        let prior = prior.filter(|prior| {
            targets.len() >= 2
                && std::ptr::eq(prior.binding, self)
                && prior.next_offset == next_offset
                && prior.model_generation == generation
                && !self.flights.is_poisoned()
                && !self.ack_ordering.is_poisoned()
        });
        let debt_projection = match prior {
            Some(prior) => match prior.debt_projection.retarget_unchanged(targets) {
                Ok(reused) => reused,
                Err(targets) => self.project_lower_debt_before_offset(next_offset, targets),
            },
            None => self.project_lower_debt_before_offset(next_offset, targets),
        };
        Some(ResponsePreparedObservation {
            binding: self,
            next_offset,
            debt_projection,
            // Retain the existing post-projection generation publication order.
            model_generation: self.response_model_generation.load(Ordering::Acquire),
        })
    }

    /// Capture the exact same targets used by Original selection, without
    /// scanning lower flights and ACK holes. Recovery discovery consumes only
    /// targets; its accepted commit independently revalidates authority.
    pub(in crate::runtime) fn observe_prepared_recovery(
        &self,
        inputs: &ResponsePreparedNativeInputs,
        lane: TrafficClass,
    ) -> Option<Vec<super::ResponseSenderPathTarget>> {
        let outputs = self
            .outputs
            .lock()
            .expect("server reliable stream binding lock");
        let targets = self.prepared_targets_locked(&outputs, inputs, lane)?;
        Some(targets)
    }

    /// Shared exact-membership and target snapshot path. Callers retain the
    /// outputs guard: Original observation projects debt before releasing it,
    /// while recovery observation returns the same target values directly.
    fn prepared_targets_locked(
        &self,
        outputs: &ResponseStreamOutputs,
        inputs: &ResponsePreparedNativeInputs,
        lane: TrafficClass,
    ) -> Option<Vec<super::ResponseSenderPathTarget>> {
        if !self.response_stream_open.load(Ordering::Acquire)
            || outputs.entries.len() != inputs.outputs.len()
            || !outputs
                .entries
                .iter()
                .zip(&inputs.outputs)
                .all(|(entry, (captured, _))| {
                    ResponseAcquisitionOutputId::from(entry) == captured.identity
                })
        {
            return None;
        }
        let ingress = *self
            .request_feedback_ingress
            .lock()
            .expect("server response ingress lock");
        let now = Instant::now();
        let targets = outputs
            .entries
            .iter()
            .zip(&inputs.outputs)
            .filter(|(entry, _)| !entry.commands.is_closed())
            .map(|(entry, (_, shape))| {
                let native = entry.key.underlay == UnderlayProtocol::Udp
                    && entry.commands.native_rate_authority().is_some();
                let snapshot = if native {
                    server_native_bulk_output_snapshot_at(
                        entry,
                        outputs.data_level_queue_bytes,
                        lane,
                        self.mux_limits,
                        *shape,
                    )
                } else {
                    server_bulk_output_snapshot_at(
                        entry,
                        outputs.data_level_queue_bytes,
                        lane,
                        self.mux_limits,
                        now,
                    )
                };
                let mut target = server_sender_path_target_at(
                    entry,
                    snapshot,
                    lane,
                    self.mux_limits,
                    ingress,
                    now,
                );
                if native {
                    target.native_authority_stamp = shape.map(|shape| shape.stamp());
                }
                target
            })
            .collect();
        Some(targets)
    }
}

#[cfg(test)]
mod tests {
    use super::super::test_support::native_response_binding_fixture;
    use super::*;
    use crate::mux::stream::ReliableSendStream;
    use crate::protocol::{Frame, StreamId};
    use crate::runtime::path::commands::{ReliablePathCommand, try_recv_reliable_path_command};
    use crate::runtime::path::prepared::PreparedOriginalClaim;
    use crate::runtime::sender::{
        PreparedResponseSource, ResponseProductState, ServerResponseSenderService,
        SharedResponseProduct, publish_prepared_response_work,
    };
    use bytes::Bytes;
    use futures::FutureExt;
    use std::sync::{Arc, Barrier};
    use std::time::Duration;

    #[test]
    fn prepared_recovery_observation_does_not_read_lower_flight_ledger() {
        let fixture = native_response_binding_fixture(8, Some(100_000_000));
        let outputs = fixture.binding.prepared_outputs();
        let inputs = ResponsePreparedNativeInputs::resolve(outputs);
        let worker_binding = fixture.binding.clone();
        let (send, receive) = std::sync::mpsc::channel();
        let flight_binding = fixture.binding.clone();
        let flights = flight_binding.flights.lock().expect("response flights");
        let worker = std::thread::spawn(move || {
            let observed = worker_binding
                .observe_prepared_recovery(&inputs, TrafficClass::Throughput)
                .map(|targets| targets.len());
            send.send(observed).expect("test receiver remains live");
        });

        let while_flights_locked = receive.recv_timeout(Duration::from_secs(1));
        let completed_while_locked = while_flights_locked.is_ok();
        drop(flights);
        let after_unlock = match while_flights_locked {
            Ok(observed) => observed,
            Err(_) => receive
                .recv_timeout(Duration::from_secs(1))
                .expect("recovery observation finishes after releasing flights"),
        };
        worker.join().expect("recovery observation worker exits");

        assert!(
            completed_while_locked,
            "target-only recovery observation must not wait for lower-debt projection locks"
        );
        assert_eq!(after_unlock, Some(1));
    }

    #[tokio::test]
    async fn prepared_response_native_claim_converts_source_without_queue_publication() {
        let mut fixture = native_response_binding_fixture(8, Some(100_000_000));
        let lane = TrafficClass::Throughput;
        let stream_id = StreamId(713);
        let quantum = 1024;
        let owner = SharedResponseProduct::new(
            ResponseProductState {
                sender: ServerResponseSenderService::new(fixture.binding.session_id(), stream_id),
                send_stream: ReliableSendStream::new(stream_id, fixture.binding.mux_limits()),
                last_send_ack: Default::default(),
                prepared: PreparedResponseSource::new(lane, quantum),
            },
            fixture.binding.clone(),
        );
        {
            let mut state = owner.lock();
            state
                .sender
                .enqueue_data_for_lane(Bytes::from(vec![0x35; 2 * quantum]), lane);
            publish_prepared_response_work(&mut state, &owner, lane, quantum, true);
            assert_eq!(state.send_stream.next_offset(), 0);
            assert_eq!(state.send_stream.reinjection_bytes(), 0);
            assert_eq!(state.sender.data_bytes(), 2 * quantum);
        }
        let work = match try_recv_reliable_path_command(&mut fixture.receivers) {
            Some(ReliablePathCommand::PreparedOriginal(work)) => work,
            _ => panic!("publication contains only a weak source notice"),
        };
        let identity = work.response_instance().unwrap();
        let frame = match work.try_claim(
            fixture
                .receivers
                .writer_ready_boundary(identity.path_instance_id)
                .unwrap(),
        ) {
            PreparedOriginalClaim::Claimed(frame) => frame,
            _ => panic!("actual current Native writer claims source"),
        };
        assert!(
            matches!(&frame, Frame::StreamData { offset: 0, payload, .. } if payload.len() == quantum)
        );
        let state = owner.lock();
        assert_eq!(state.send_stream.next_offset(), quantum as u64);
        assert_eq!(state.send_stream.reinjection_bytes(), quantum);
        assert_eq!(state.sender.data_bytes(), quantum);
        assert_eq!(
            state.prepared.first_claimed_at,
            state.prepared.last_claimed_at
        );
        assert_eq!(
            fixture
                .binding
                .original_flight_outputs_overlapping_frame(&frame)
                .len(),
            1
        );
        drop(state);
        assert!(
            try_recv_reliable_path_command(&mut fixture.receivers).is_none(),
            "claim does not enqueue a Data command"
        );
        let charge = fixture.receivers.register_claimed_writer_frame(&frame);
        fixture.receivers.release_pending_command_bytes(charge);
    }

    #[tokio::test]
    async fn prepared_response_native_fenced_busy_releases_native_before_owner_wait() {
        let fixture = native_response_binding_fixture(8, Some(100_000_000));
        let owner = SharedResponseProduct::new(
            ResponseProductState {
                sender: ServerResponseSenderService::new(
                    fixture.binding.session_id(),
                    StreamId(714),
                ),
                send_stream: ReliableSendStream::new(StreamId(714), fixture.binding.mux_limits()),
                last_send_ack: Default::default(),
                prepared: PreparedResponseSource::new(TrafficClass::Throughput, 1024),
            },
            fixture.binding.clone(),
        );
        let stamp = fixture
            .authority
            .scheduling_shape_snapshot(fixture.scope)
            .unwrap()
            .stamp();
        let guard = owner.lock();
        let entered = Arc::new(Barrier::new(2));
        let proceed = Arc::new(Barrier::new(2));
        let writer_owner = owner.clone();
        let writer_native = fixture.authority.clone();
        let writer_entered = entered.clone();
        let writer_proceed = proceed.clone();
        let writer = std::thread::spawn(move || {
            let attempt = writer_owner.arm_claim();
            writer_native
                .commit_with_current_scheduling_shape(stamp, |_| {
                    writer_entered.wait();
                    writer_proceed.wait();
                    match attempt.try_lock() {
                        Ok(_) => panic!("actor still owns source"),
                        Err(wait) => wait,
                    }
                })
                .unwrap()
        });
        entered.wait();
        proceed.wait();
        assert_eq!(
            fixture
                .authority
                .scheduling_shape_snapshot(fixture.scope)
                .unwrap()
                .stamp(),
            stamp
        );
        let wait = writer.join().expect("writer releases Native on Busy");
        assert_eq!(guard.send_stream.next_offset(), 0);
        drop(guard);
        assert_eq!(
            wait.now_or_never(),
            Some(()),
            "unlock before first poll remains visible"
        );
    }

    fn two_tcp_output_binding() -> (
        Arc<ResponseStreamBinding>,
        crate::model::path::CarrierPathKey,
        Vec<crate::runtime::path::commands::ReliablePathCommandReceivers>,
    ) {
        use crate::protocol::PathId;
        use crate::runtime::path::commands::reliable_path_command_channels;
        let (binding, key, first) =
            super::super::test_support::binding_for_underlay(UnderlayProtocol::Tcp);
        let (commands, second) = reliable_path_command_channels(8);
        binding.attach(
            UnderlayProtocol::Tcp,
            PathId(11),
            commands,
            TrafficClass::Throughput,
        );
        (binding, key, vec![first, second])
    }

    fn assert_same_debt(a: &ResponsePreparedObservation<'_>, b: &ResponsePreparedObservation<'_>) {
        assert_eq!(
            a.debt_projection.oldest_owner(),
            b.debt_projection.oldest_owner()
        );
        assert_eq!(
            a.debt_projection.targets().len(),
            b.debt_projection.targets().len()
        );
        for i in 0..a.debt_projection.targets().len() {
            assert_eq!(
                a.debt_projection.exact_other_path_debt_bytes(i),
                b.debt_projection.exact_other_path_debt_bytes(i)
            );
        }
    }

    #[test]
    fn final_original_reuses_only_fenced_lower_range_facts() {
        let (binding, key, _receivers) = two_tcp_output_binding();
        binding.record_original_flight(
            key,
            &Frame::StreamData {
                stream_id: StreamId(42),
                offset: 0,
                payload: Bytes::from_static(b"abcdefgh"),
            },
        );
        let inputs = ResponsePreparedNativeInputs::resolve(binding.prepared_outputs());
        let prior = binding
            .observe_prepared_original(&inputs, TrafficClass::Throughput, 64)
            .unwrap();
        let expected = binding
            .observe_prepared_original(&inputs, TrafficClass::Throughput, 64)
            .unwrap();
        std::thread::scope(|scope| {
            let flights = binding.flights.lock().expect("test flights");
            let holes = binding.ack_ordering.lock().expect("test holes");
            let (send, receive) = std::sync::mpsc::channel();
            let binding = &binding;
            let inputs = &inputs;
            let expected = &expected;
            let worker = scope.spawn(move || {
                let observed = binding
                    .reobserve_prepared_original(inputs, TrafficClass::Throughput, 64, prior)
                    .unwrap();
                assert_same_debt(&observed, expected);
                send.send(()).unwrap();
            });
            let completed_while_locked = receive.recv_timeout(Duration::from_secs(1)).is_ok();
            drop(holes);
            drop(flights);
            worker.join().unwrap();
            assert!(
                completed_while_locked,
                "same-generation final observation must not rescan"
            );
        });
        let prior = binding
            .observe_prepared_original(&inputs, TrafficClass::Throughput, 64)
            .unwrap();
        let prior_generation = prior.model_generation;
        binding.record_original_flight(
            key,
            &Frame::StreamData {
                stream_id: StreamId(42),
                offset: 16,
                payload: Bytes::from_static(b"ijkl"),
            },
        );
        let after = binding
            .reobserve_prepared_original(&inputs, TrafficClass::Throughput, 64, prior)
            .unwrap();
        let fresh = binding
            .observe_prepared_original(&inputs, TrafficClass::Throughput, 64)
            .unwrap();
        assert_same_debt(&after, &fresh);
        assert_ne!(after.model_generation, prior_generation);
        let prefix = binding
            .reobserve_prepared_original(&inputs, TrafficClass::Throughput, 4, after)
            .unwrap();
        assert_same_debt(
            &prefix,
            &binding
                .observe_prepared_original(&inputs, TrafficClass::Throughput, 4)
                .unwrap(),
        );
    }

    #[test]
    fn receipt_from_another_binding_misses_even_at_the_current_generation() {
        let (foreign_binding, _, _foreign_receivers) = two_tcp_output_binding();
        let (binding, key, _receivers) = two_tcp_output_binding();
        let inputs = ResponsePreparedNativeInputs::resolve(binding.prepared_outputs());
        let stale = binding
            .observe_prepared_original(&inputs, TrafficClass::Throughput, 64)
            .unwrap();

        binding.record_original_flight(
            key,
            &Frame::StreamData {
                stream_id: StreamId(42),
                offset: 0,
                payload: Bytes::from_static(b"abcdefgh"),
            },
        );
        // Deliberately set the receipt generation to the target binding's current
        // generation. This isolates the binding-pointer fence from generation,
        // offset, and ordered-identity fences.
        let foreign_receipt = ResponsePreparedObservation {
            binding: foreign_binding.as_ref(),
            next_offset: 64,
            debt_projection: stale.debt_projection,
            model_generation: binding.response_model_generation(),
        };

        let observed = binding
            .reobserve_prepared_original(&inputs, TrafficClass::Throughput, 64, foreign_receipt)
            .unwrap();
        let reference = binding
            .observe_prepared_original(&inputs, TrafficClass::Throughput, 64)
            .unwrap();
        assert_same_debt(&observed, &reference);
        let oldest = observed.debt_projection.oldest_owner();
        assert!(oldest.is_some());
        assert_eq!(oldest.map(|(owner, _)| owner), Some(key));
    }

    #[test]
    fn original_receipt_misses_after_ack_releases_lower_geometry() {
        let (binding, key, _receivers) = two_tcp_output_binding();
        binding.record_original_flight(
            key,
            &Frame::StreamData {
                stream_id: StreamId(42),
                offset: 0,
                payload: Bytes::from_static(b"abcdefgh"),
            },
        );
        let inputs = ResponsePreparedNativeInputs::resolve(binding.prepared_outputs());
        let prior = binding
            .observe_prepared_original(&inputs, TrafficClass::Throughput, 64)
            .unwrap();
        let prior_generation = prior.model_generation;
        assert!(prior.debt_projection.oldest_owner().is_some());

        binding.release_normalized_acked_ranges(&[
            crate::protocol::OffsetRange::new(0, 8).expect("valid ACK range")
        ]);
        assert_ne!(binding.response_model_generation(), prior_generation);

        let observed = binding
            .reobserve_prepared_original(&inputs, TrafficClass::Throughput, 64, prior)
            .unwrap();
        let reference = binding
            .observe_prepared_original(&inputs, TrafficClass::Throughput, 64)
            .unwrap();
        assert_same_debt(&observed, &reference);
        assert_eq!(observed.debt_projection.oldest_owner(), None);
    }

    #[test]
    fn original_receipt_misses_after_attachment_membership_changes() {
        let (binding, _, _receivers) = two_tcp_output_binding();
        let old_inputs = ResponsePreparedNativeInputs::resolve(binding.prepared_outputs());
        let prior = binding
            .observe_prepared_original(&old_inputs, TrafficClass::Throughput, 64)
            .unwrap();
        let prior_generation = prior.model_generation;

        let (commands, _new_receivers) =
            crate::runtime::path::commands::reliable_path_command_channels(8);
        assert_eq!(
            binding.attach(
                UnderlayProtocol::Tcp,
                crate::protocol::PathId(12),
                commands,
                TrafficClass::Throughput,
            ),
            super::super::ResponseStreamAttachOutcome::Attached,
        );
        assert_ne!(binding.response_model_generation(), prior_generation);

        // The prior native inputs no longer describe full membership. Capture the
        // current exact set, then verify the old receipt falls back for its changed
        // generation and target count.
        let current_inputs = ResponsePreparedNativeInputs::resolve(binding.prepared_outputs());
        let observed = binding
            .reobserve_prepared_original(&current_inputs, TrafficClass::Throughput, 64, prior)
            .unwrap();
        let reference = binding
            .observe_prepared_original(&current_inputs, TrafficClass::Throughput, 64)
            .unwrap();
        assert_same_debt(&observed, &reference);
        assert_eq!(observed.debt_projection.targets().len(), 3);
    }

    #[test]
    fn single_target_original_receipt_keeps_the_reference_scan() {
        use std::sync::TryLockError;
        use std::sync::mpsc;
        use std::time::Instant;

        let (binding, key, _receivers) =
            super::super::test_support::binding_for_underlay(UnderlayProtocol::Tcp);
        binding.record_original_flight(
            key,
            &Frame::StreamData {
                stream_id: StreamId(42),
                offset: 0,
                payload: Bytes::from_static(b"abcdefgh"),
            },
        );
        let inputs = ResponsePreparedNativeInputs::resolve(binding.prepared_outputs());
        let prior = binding
            .observe_prepared_original(&inputs, TrafficClass::Throughput, 64)
            .unwrap();
        assert_eq!(prior.debt_projection.targets().len(), 1);

        std::thread::scope(|scope| {
            let flights = binding.flights.lock().expect("test flights");
            let (started_send, started_receive) = mpsc::channel();
            let (done_send, done_receive) = mpsc::channel();
            let worker_binding = &binding;
            let worker = scope.spawn(move || {
                started_send.send(()).unwrap();
                let observed = worker_binding
                    .reobserve_prepared_original(&inputs, TrafficClass::Throughput, 64, prior)
                    .unwrap();
                assert!(done_send.send(observed).is_ok());
            });
            started_receive.recv().expect("worker started");

            // Wait until the worker owns outputs and therefore has reached the
            // reference projection's blocked flight-lock acquisition.
            let deadline = Instant::now() + Duration::from_secs(1);
            loop {
                match binding.outputs.try_lock() {
                    Ok(outputs) => drop(outputs),
                    Err(TryLockError::WouldBlock) => break,
                    Err(TryLockError::Poisoned(_)) => panic!("outputs lock poisoned"),
                }
                assert!(Instant::now() < deadline, "worker did not acquire outputs");
                std::thread::yield_now();
            }
            let completed_while_flights_locked =
                done_receive.recv_timeout(Duration::from_millis(30)).is_ok();
            drop(flights);
            let observed = done_receive
                .recv_timeout(Duration::from_secs(1))
                .expect("reference scan finishes after flights unlock");
            worker.join().expect("worker exits");
            assert!(
                !completed_while_flights_locked,
                "one target must keep the established lower-flight scan",
            );
            let reference = binding
                .observe_prepared_original(
                    &ResponsePreparedNativeInputs::resolve(binding.prepared_outputs()),
                    TrafficClass::Throughput,
                    64,
                )
                .unwrap();
            assert_same_debt(&observed, &reference);
        });
    }

    #[test]
    fn original_receipt_preserves_poisoned_lower_state_failure() {
        for poison_ordering in [false, true] {
            for reuse in [false, true] {
                let (binding, key, _receivers) = two_tcp_output_binding();
                binding.record_original_flight(
                    key,
                    &Frame::StreamData {
                        stream_id: StreamId(42),
                        offset: 0,
                        payload: Bytes::from_static(b"abcdefgh"),
                    },
                );
                let inputs = ResponsePreparedNativeInputs::resolve(binding.prepared_outputs());
                let prior = binding
                    .observe_prepared_original(&inputs, TrafficClass::Throughput, 64)
                    .unwrap();
                let poison = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    if poison_ordering {
                        let _guard = binding.ack_ordering.lock().unwrap();
                        panic!("isolated ordering poison");
                    } else {
                        let _guard = binding.flights.lock().unwrap();
                        panic!("isolated flight poison");
                    }
                }));
                assert!(poison.is_err());
                let observed = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    if reuse {
                        binding.reobserve_prepared_original(
                            &inputs,
                            TrafficClass::Throughput,
                            64,
                            prior,
                        )
                    } else {
                        binding.observe_prepared_original(&inputs, TrafficClass::Throughput, 64)
                    }
                }));
                assert!(
                    observed.is_err(),
                    "reuse must preserve the reference poison failure"
                );
            }
        }
    }
}
